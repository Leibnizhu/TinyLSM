package io.github.leibnizhu.tinylsm

import io.github.leibnizhu.tinylsm.block.BlockCache
import io.github.leibnizhu.tinylsm.compact.{CompactionController, FullCompactionTask}
import io.github.leibnizhu.tinylsm.iterator.*
import io.github.leibnizhu.tinylsm.utils.*
import org.slf4j.LoggerFactory

import java.io.File
import java.util
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.boundary

object LsmStorageInner {
  private val log = LoggerFactory.getLogger(this.getClass)

  def apply(path: File, options: LsmStorageOptions): LsmStorageInner = {
    val state = LsmStorageState(options)
    val nextSstId = AtomicInteger(0)
    val blockCache = BlockCache.apply(128)
    val compactionController = CompactionController(options.compactionOptions)
    val manifestFile = new File(path, "MANIFEST")
    val manifest = new Manifest(manifestFile, options.targetManifestSize)
    if (manifestFile.exists()) {
      // manifest 已存在则需要恢复 LSM 状态
      val records = manifest.recover()
      val memTables = new mutable.HashSet[Int]()
      // 根据 manifest的记录顺序重做 state。注意 ssTables 还是空的，这一步先不处理
      for (record <- records) record match
        case ManifestSnapshot(frozenMt, l0, levels) =>
          memTables ++= frozenMt
          state.l0SsTables = l0
          state.levels = levels
        case ManifestNewMemtable(memtableId) =>
          nextSstId.set(nextSstId.get().max(memtableId))
          memTables += memtableId
        case ManifestFlush(sstId) =>
          assert(memTables(sstId), "memtable not exist?")
          memTables -= sstId
          if (compactionController.flushToL0()) {
            state.l0SsTables = sstId :: state.l0SsTables
          } else {
            state.levels = (sstId, List(sstId)) :: state.levels
          }
        case ManifestCompaction(task, output) =>
          task.applyCompactionResult(state, output)
          nextSstId.set(nextSstId.get().max(output.max))

      // 恢复 SST ssTables
      var sstCnt = 0
      for (sstId <- state.l0SsTables ++ state.levels.flatMap(_._2)) {
        val sstFileObj = FileObject.open(fileOfSst(path, sstId))
        val sst = SsTable.open(sstId, Some(blockCache), sstFileObj)
        state.ssTables = state.ssTables + (sstId -> sst)
        sstCnt += 1
      }
      log.info("{} SST opened", sstCnt)

      val newMemtableId = nextSstId.get()
      if (options.enableWal) {
        // 从 wal 恢复Memtable,到了这里，memTables 里面是从Manifest恢复的、没flush的Memtable
        var walCnt = 0
        for (mtId <- memTables) {
          val memTable = MemTable.recoverFromWal(mtId, fileOfWal(path, mtId))
          if (memTable.nonEmpty) {
            state.immutableMemTables = memTable :: state.immutableMemTables
            walCnt += 1
          }
        }
        log.info("{} MemTable recovered from wal", walCnt)
        state.memTable = MemTable(newMemtableId, Some(fileOfWal(path, newMemtableId)))
      } else {
        // 由于前面恢复了SST，所以此时要更新 Memtable
        state.memTable = MemTable(newMemtableId)
      }
      manifest.addRecord(ManifestNewMemtable(state.memTable.id))
      nextSstId.getAndIncrement()
    } else {
      // manifest 不存在，则直接创建新的
      if (options.enableWal) {
        //  用WAL重新创建Memtable
        val memTableId = state.memTable.id
        state.memTable = MemTable(memTableId, Some(fileOfWal(path, memTableId)))
      }
      manifest.addRecord(ManifestNewMemtable(state.memTable.id))
    }
    log.info("Start LsmStorageInner with lsm dir: {}", path.getAbsolutePath)
    new LsmStorageInner(path, state, blockCache, options, nextSstId, compactionController, Some(manifest))
  }

  def fileOfWal(path: File, walId: Int): File = new File(path, "%05d.wal".format(walId))

  def fileOfSst(path: File, sstId: Int): File = new File(path, "%05d.sst".format(sstId))
}

private[tinylsm] class LsmStorageInner(
                                        // TinyLSM 的根目录
                                        path: File,
                                        val state: LsmStorageState,
                                        val blockCache: BlockCache,
                                        val options: LsmStorageOptions,
                                        val nextSstId: AtomicInteger,
                                        val compactionController: CompactionController,
                                        val manifest: Option[Manifest] = None) {
  private val log = LoggerFactory.getLogger(this.getClass)

  /**
   * 按key获取
   *
   * @param key key
   * @return 可能为None
   */
  def get(key: MemTableKey): Option[MemTableValue] = {
    getWithTs(key)
    /*assert(key != null && !key.isEmpty, "key cannot be empty")

    val snapshot = state.read(_.copy())

    // 先判断当前未 freeze 的MemTable是否有需要读取的值
    val inMemTable = snapshot.memTable.get(key)
    if (inMemTable.isDefined) {
      // 如果读取出来是墓碑（空Array）需要过滤返回None
      return inMemTable.filter(!_.sameElements(DELETE_TOMBSTONE))
    }

    // 由新到旧遍历已 freeze 的MemTable，找到直接返回
    val inFrozenMemTable = {
      boundary:
        for (mt <- snapshot.immutableMemTables) do {
          val curValue = mt.get(key)
          if (curValue.isDefined) {
            // 如果读取出来是墓碑（空Array）需要过滤返回None
            boundary.break(curValue.filter(!_.sameElements(DELETE_TOMBSTONE)))
          }
        }
        None
    }
    if (inFrozenMemTable.isDefined) {
      return inFrozenMemTable
    }

    // 从 SST读取
    // l0 sst 的多个sst从key开始构成一个 MergeIterator 可一查
    val l0SsTableIters = snapshot.l0SsTables
      .map(snapshot.ssTables(_))
      .filter(_.mayContainsKey(key))
      .map(SsTableIterator.createAndSeekToKey(_, key))
    // levels sst的读取
    val levelIters = snapshot.levels.map((_, levelSstIds) => {
      val levelSsts = levelSstIds
        .map(snapshot.ssTables(_))
        .filter(_.mayContainsKey(key))
      SstConcatIterator.createAndSeekToKey(levelSsts, key)
    })
    // 合成sst迭代器
    val sstIter = TwoMergeIterator(MergeIterator(l0SsTableIters), MergeIterator(levelIters))
    if (sstIter.isValid && sstIter.key().equals(key) && !sstIter.value().sameElements(DELETE_TOMBSTONE)) {
      // l0 sst 有效、有当前查询的key、且值不为空，即找到了value
      return Some(sstIter.value())
    }
    None*/
  }

  def getWithTs(key: MemTableKey): Option[MemTableValue] = {
    assert(key != null && !key.isEmpty, "key cannot be empty")
    val snapshot = state.read(_.copy())

    // Memtable 部分的迭代器
    val curMemtableIter = snapshot.memTable.scan(
      Included(MemTableKey.withBeginTs(key)), Included(MemTableKey.withEndTs(key)))
    val frozenMemtableIters = snapshot.immutableMemTables.map(im =>
      im.scan(Included(MemTableKey.withBeginTs(key)), Included(MemTableKey.withEndTs(key))))
    val memtableIter = MergeIterator(curMemtableIter :: frozenMemtableIters)

    // L0 部分的迭代器
    val l0Iter = MergeIterator(snapshot.l0SsTables
      .map(snapshot.ssTables(_))
      .filter(sst => sst.mayContainsKey(key))
      .map(SsTableIterator.createAndSeekToKey(_, MemTableKey.withBeginTs(key))))

    // 其他 level 部分的迭代器
    val levelIters = snapshot.levels.map((_, levelSstIds) => {
      // 当前level可能包含指定key的SST
      val levelSsts = levelSstIds.map(snapshot.ssTables(_)).filter(_.mayContainsKey(key))
      SstConcatIterator.createAndSeekToKey(levelSsts, MemTableKey.withBeginTs(key))
    })

    // 合成最终的迭代器
    val finalIter = LsmIterator(
      TwoMergeIterator(TwoMergeIterator(memtableIter, l0Iter), MergeIterator(levelIters)),
      Unbounded()
    )

    if (finalIter.isValid && finalIter.key().equalsOnlyKey(key) && 
      !finalIter.value().sameElements(DELETE_TOMBSTONE)) {
      Some(finalIter.value())
    } else {
      None
    }
  }


  def get(key: String): Option[String] = get(MemTableKey(key.getBytes)).map(new String(_))

  /**
   * 插入或更新
   *
   * @param key   key
   * @param value 如果要执行delete操作，可以传入null
   */
  def put(key: MemTableKey, value: MemTableValue): Unit = writeBatch(Array(WriteBatchRecord.Put(key, value)))

  def put(key: String, value: String): Unit = put(MemTableKey(key.getBytes), value.getBytes)

  /**
   * 按key删除
   *
   * @param key key
   */
  def delete(key: MemTableKey): Unit = writeBatch(Array(WriteBatchRecord.Del(key)))

  def delete(key: String): Unit = delete(MemTableKey(key.getBytes))

  def writeBatch(batch: Seq[WriteBatchRecord]): Unit = for (record <- batch) {
    record match
      case WriteBatchRecord.Del(key: MemTableKey) =>
        assert(key != null && !key.isEmpty, "key cannot be empty")
        // 这里 MemTable 自己的线程安全由 ConcurrentHashMap 保证，所以只要读锁
        val estimatedSize = state.read(st => {
          st.memTable.put(key, DELETE_TOMBSTONE)
          st.memTable.approximateSize.get()
        })
        tryFreezeMemTable(estimatedSize)
      case WriteBatchRecord.Put(key: MemTableKey, value: MemTableValue) =>
        assert(key != null && !key.isEmpty, "key cannot be empty")
        assert(value != null, "value cannot be empty")
        // 这里 MemTable 自己的线程安全由 ConcurrentHashMap 保证，所以只要读锁
        val estimatedSize = state.read(st => {
          st.memTable.put(key, value)
          st.memTable.approximateSize.get()
        })
        tryFreezeMemTable(estimatedSize)
  }

  private def doPut(key: MemTableKey, value: MemTableValue): Unit = {
  }

  private def tryFreezeMemTable(estimatedSize: Int): Unit = {
    if (estimatedSize >= options.targetSstSize) {
      try {
        state.stateLock.lock()
        // 双重校验，因为前面是用读锁进来的，当多个线程同时在 doPut() 读取都是超了targetSstSize大小的时候
        // 还需要一个普通锁限制只能有一个线程做 freeze 的操作，同时进来的其他线程需要等待 stateLock
        // 而 freeze 操作会上写锁，其他线程会在 doPut() 等待读锁
        if (state.read(st => st.memTable.approximateSize.get() >= this.options.targetSstSize)) {
          this.forceFreezeMemTable()
        }
      } finally {
        state.stateLock.unlock()
      }
    }
  }

  /**
   * 冻结当前MemTable，创建一个新的
   */
  def forceFreezeMemTable(): Unit = {
    if (state.read(_.memTable.isEmpty)) {
      log.info("Current MemTable is empty, skip freezing")
      return
    }
    val newMemTableId = nextSstId.incrementAndGet()
    val newMemTable = if (options.enableWal) MemTable(newMemTableId, Some(fileOfWal(newMemTableId))) else MemTable(newMemTableId)
    freezeMemTableWithMemTable(newMemTable)
    manifest.foreach(_.addRecord(ManifestNewMemtable(newMemTableId)))
  }

  def freezeMemTableWithMemTable(newMemTable: MemTable): Unit = {
    state.write(st => {
      val oldMemTable = st.memTable
      st.memTable = newMemTable
      st.immutableMemTables = oldMemTable :: st.immutableMemTables

      oldMemTable.syncWal()
    })
  }

  private def fileOfWal(walId: Int): File = LsmStorageInner.fileOfWal(path, walId)

  private def fileOfSst(sstId: Int): File = LsmStorageInner.fileOfSst(path, sstId)

  /**
   * 强制将最早的一个ImmutableMemTable 刷入磁盘
   */
  def forceFlushNextImmutableMemTable(): Unit = {
    if (state.immutableMemTables.isEmpty) {
      log.info("No frozen MemTable, skip flush")
      return
    }
    try {
      state.stateLock.lock()
      val flushMemTable = state.immutableMemTables.last
      // 构建SST、写入SST文件
      val builder = SsTableBuilder(options.blockSize)
      flushMemTable.flush(builder)
      val sstId = flushMemTable.id
      val sst = builder.build(sstId, Some(blockCache), fileOfSst(sstId))

      // 移除MemTable、加入到L0 table
      state.immutableMemTables = state.immutableMemTables.slice(0, state.immutableMemTables.length - 1)
      if (compactionController.flushToL0()) {
        state.l0SsTables = sstId :: state.l0SsTables
      } else {
        // Tiered compaction不能flush到L0, 每次都写到levels的最前面，Tier ID是对应第一个SST的ID
        state.levels = (sstId, List(sstId)) :: state.levels
      }
      state.ssTables = state.ssTables + (sstId -> sst)

      // 删除WAL文件
      if (options.enableWal) {
        val deleteWal = fileOfWal(sstId).delete()
        log.info("Deleted WAL file {} {}", fileOfWal(sstId).getName, if (deleteWal) "success" else "failed")
      }
      manifest.foreach(_.addRecord(ManifestFlush(sstId)))
    } finally {
      state.stateLock.unlock()
    }
  }

  def scan(lower: Bound, upper: Bound): FusedIterator[MemTableKey, MemTableValue] = {
    val memTableIters = ArrayBuffer[MemTableIterator]()
    val snapshot = state.read(_.copy())
    memTableIters.addOne(snapshot.memTable.scan(lower, upper))
    snapshot.immutableMemTables.map(mt => mt.scan(lower, upper)).foreach(memTableIters.addOne)
    val memTablesIter = MergeIterator(memTableIters.toList)
    val ssTableIters = snapshot.l0SsTables.map(snapshot.ssTables(_))
      // 过滤key范围可能包含当前scan范围的sst，减少IO
      .filter(sst => rangeOverlap(lower, upper, sst.firstKey, sst.lastKey))
      .map(sst => SsTableIterator.createByLowerBound(sst, lower))
    val l0ssTablesIter = MergeIterator(ssTableIters)
    // 处理 levels
    val levelIters = snapshot.levels.map((_, levelSstIds) => {
      val levelSsts = levelSstIds
        .map(snapshot.ssTables(_))
        .filter(sst => rangeOverlap(lower, upper, sst.firstKey, sst.lastKey))
      SstConcatIterator.createByLowerBound(levelSsts, lower)
    })
    val levelTablesIter = MergeIterator(levelIters)
    val mergedIter = TwoMergeIterator(TwoMergeIterator(memTablesIter, l0ssTablesIter), levelTablesIter)
    FusedIterator(LsmIterator(mergedIter, upper))
  }


  def forceFullCompaction(): Unit = {
    /*if (options.compactionOptions == CompactionOptions.NoCompaction) {
      throw new IllegalStateException("full compaction can only be called with compaction is enabled")
    }*/

    // 执行 full compaction
    val snapshot = this.state.read(_.copy())
    val l0SsTables = List(snapshot.l0SsTables: _*)
    val l1SsTables = if (snapshot.levels.isEmpty) List() else List(snapshot.levels.head._2: _*)
    val compactionTask = FullCompactionTask(l0SsTables, l1SsTables)
    log.info("Force full compaction: {}", compactionTask)
    val newSsTables = compactionTask.doCompact(this)
    val newSstIds = newSsTables.map(_.sstId())
    val sstToRemove = compactionController.applyCompactionToState(state, newSsTables, compactionTask)
    manifest.foreach(_.addRecord(ManifestCompaction(compactionTask, newSstIds)))
    // 释放锁之后再做文件IO
    deleteSstFiles(sstToRemove)
    log.info("force full compaction done, new SSTs: {}", newSstIds)
  }

  /**
   * 使用迭代器迭代数据，写入新sst
   *
   * @param iter                 迭代器
   * @param compactToBottomLevel 是否压缩合并底部的level，如果是的话，会删除 delete墓碑
   * @return 新的sst
   */
  def compactGenerateSstFromIter(iter: MemTableStorageIterator, compactToBottomLevel: Boolean): List[SsTable] = {
    var builder: Option[SsTableBuilder] = None
    val newSstList = new ArrayBuffer[SsTable]()
    while (iter.isValid) {
      if (builder.isEmpty) {
        builder = Some(SsTableBuilder(options.blockSize))
      }
      val innerBuilder = builder.get
      if (compactToBottomLevel) {
        // 如果压缩合并底部的level，则不保留delete墓碑
        if (!iter.value().sameElements(DELETE_TOMBSTONE)) {
          innerBuilder.add(iter.key(), iter.value())
        }
      } else {
        innerBuilder.add(iter.key(), iter.value())
      }
      iter.next()
      // builder满了则生成sst，并新开一个builder
      if (innerBuilder.estimateSize() >= options.targetSstSize) {
        val sstId = nextSstId.incrementAndGet()
        val sst = innerBuilder.build(sstId, Some(blockCache), fileOfSst(sstId))
        builder = None
        newSstList += sst
      }
    }
    // 最后一个SsTableBuilder
    if (builder.isDefined) {
      val sstId = nextSstId.incrementAndGet()
      val sst = builder.get.build(sstId, Some(blockCache), fileOfSst(sstId))
      newSstList += sst
    }
    newSstList.toList
  }

  def newTxn(): Unit = {

  }

  /**
   * sst的范围是否包含用户指定的scan范围
   *
   * @param userBegin scan指定的左边界
   * @param userEnd   scan指定的右边界
   * @param sstBegin  sst的左边，第一个key
   * @param sstEnd    sst的右边，最后一个key
   * @return sst是否满足scan范围
   */
  private def rangeOverlap(userBegin: Bound, userEnd: Bound,
                           sstBegin: MemTableKey, sstEnd: MemTableKey): Boolean = {
    // 判断scan的右边界如果小于SST的最左边第一个key，那么这个sst肯定不包含这个scan范围
    userEnd match
      case Excluded(r: MemTableKey) if r.compareTo(sstBegin) <= 0 => return false
      case Included(r: MemTableKey) if r.compareTo(sstBegin) < 0 => return false
      case _ =>
    // 判断scan的左边界如果大于SST的最右边最后一个key，那么这个sst肯定不包含这个scan范围
    userBegin match
      case Excluded(r: MemTableKey) if r.compareTo(sstEnd) >= 0 => return false
      case Included(r: MemTableKey) if r.compareTo(sstEnd) > 0 => return false
      case _ =>
    true
  }

  def dumpStorage(): Unit = {
    val itr = scan(Unbounded(), Unbounded())
    print("Storage content: ")
    while (itr.isValid) {
      print(s"${new String(itr.key().bytes)}@${itr.key().ts} => ${new String(itr.value())}, ")
      itr.next()
    }
    println()
  }

  def dumpState(): Unit = state.dumpState()

  def triggerFlush(): Unit = {
    val needTrigger = state.read(st => st.immutableMemTables.length >=
      options.numMemTableLimit)
    if (needTrigger) {
      log.info("Trigger flush earliest MemTable to SST...")
      forceFlushNextImmutableMemTable()
    }
  }

  def triggerCompact(): Unit = {
    val snapshot = state.read(_.copy())
    val compactTask = compactionController.generateCompactionTask(snapshot)
    if (compactTask.isEmpty) {
      return
    }
    val task = compactTask.get
    log.info("Running compaction task: {}", task)
    dumpState()
    val newSsTables = task.doCompact(this)
    val newSstIds = newSsTables.map(_.sstId())
    log.info("Compaction task finished: {}, new SST: {}", task, newSstIds)
    val sstToRemove = compactionController.applyCompactionToState(state, newSsTables, task)
    manifest.foreach(_.addRecord(ManifestCompaction(task, newSstIds)))
    // 释放锁之后再做文件IO
    deleteSstFiles(sstToRemove)
  }

  def triggerManifestCompact(): Unit = {
    manifest.foreach(_.tryCompact(state.read(_.copy())))
  }

  private def deleteSstFiles(sstIds: List[Int]): Unit = {
    for (sstId <- sstIds) {
      val sstFile = fileOfSst(sstId)
      val deleted = sstFile.delete()
      log.info("Deleted SST table file: {} {}", sstFile.getName, if (deleted) "success" else "failed")
    }
  }

  def syncWal(): Unit = state.memTable.syncWal()
}

enum WriteBatchRecord {
  case Del(key: MemTableKey) extends WriteBatchRecord

  case Put(key: MemTableKey, value: MemTableValue) extends WriteBatchRecord
}