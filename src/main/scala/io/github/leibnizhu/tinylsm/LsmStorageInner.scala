package io.github.leibnizhu.tinylsm

import io.github.leibnizhu.tinylsm.MemTableKey.{TS_MAX, TS_RANGE_END}
import io.github.leibnizhu.tinylsm.block.BlockCache
import io.github.leibnizhu.tinylsm.compact.{CompactionController, CompactionFilter, FullCompactionTask}
import io.github.leibnizhu.tinylsm.compress.SsTableCompressor
import io.github.leibnizhu.tinylsm.iterator.*
import io.github.leibnizhu.tinylsm.mvcc.{LsmMvccInner, Transaction}
import io.github.leibnizhu.tinylsm.utils.*
import org.slf4j.LoggerFactory

import java.io.File
import java.util
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.*
import scala.math.Ordering.Implicits.infixOrderingOps

object LsmStorageInner {
  private val log = LoggerFactory.getLogger(this.getClass)

  def apply(path: File, options: LsmStorageOptions): LsmStorageInner = {
    val state = LsmStorageState(options)
    val nextSstId = AtomicInteger(0)
    val blockCache = BlockCache.apply(128)
    val compactionController = CompactionController(options.compactionOptions)
    val manifestFile = new File(path, "MANIFEST")
    val manifest = new Manifest(manifestFile, options.targetManifestSize)
    var lastCommitTs = 0L;
    if (manifestFile.exists()) {
      // manifest 已存在则需要恢复 LSM 状态
      val records = manifest.recover()
      val memTables = new mutable.HashSet[Int]()
      // 根据 manifest的记录顺序重做 state。注意 ssTables 还是空的，这一步先不处理
      for (record <- records) record match
        case ManifestSnapshot(curMut, frozenMt, l0, levels) =>
          memTables ++= frozenMt += curMut
          nextSstId.set(nextSstId.get().max(curMut))
          if (frozenMt.nonEmpty) {
            nextSstId.set(nextSstId.get().max(frozenMt.max))
          }
          state.l0SsTables = l0
          state.levels = levels
        case ManifestNewMemtable(memtableId) =>
          nextSstId.set(nextSstId.get().max(memtableId))
          memTables += memtableId
        case ManifestFlush(sstId) =>
          assert(memTables(sstId), s"memtable $sstId not exist? All memtables: $memTables")
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
        lastCommitTs = lastCommitTs.max(sst.maxTimestamp)
        state.ssTables = state.ssTables + (sstId -> sst)
        sstCnt += 1
      }
      log.info("{} SST opened", sstCnt)

      val newMemtableId = nextSstId.incrementAndGet()
      if (options.enableWal) {
        // 从 wal 恢复Memtable,到了这里，memTables 里面是从Manifest恢复的、没flush的Memtable
        var walCnt = 0
        for (mtId <- memTables) {
          val memTable = MemTable.recoverFromWal(mtId, fileOfWal(path, mtId))
          if (memTable.nonEmpty) {
            state.immutableMemTables = memTable :: state.immutableMemTables
            val memTableMaxTs = memTable.map.keySet().asScala.map(_.ts).max
            lastCommitTs = lastCommitTs.max(memTableMaxTs)
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
    log.info("Start LsmStorageInner with lsm dir: {}, last commit ts: {}", path.getAbsolutePath, lastCommitTs)
    val mvcc = LsmMvccInner(lastCommitTs)
    new LsmStorageInner(path, state, blockCache, options, nextSstId, compactionController, Some(manifest), Some(mvcc))
  }

  private def fileOfWal(path: File, walId: Int): File = new File(path, "%05d.wal".format(walId))

  private def fileOfSst(path: File, sstId: Int): File = new File(path, "%05d.sst".format(sstId))
}

private[tinylsm] case class LsmStorageInner(
                                             // TinyLSM 的根目录
                                             path: File,
                                             val state: LsmStorageState,
                                             val blockCache: BlockCache,
                                             val options: LsmStorageOptions,
                                             val nextSstId: AtomicInteger,
                                             val compactionController: CompactionController,
                                             val manifest: Option[Manifest] = None,
                                             val mvcc: Option[LsmMvccInner] = None,
                                             val compactionFilters: ConcurrentLinkedDeque[CompactionFilter] = new ConcurrentLinkedDeque()
                                           ) {
  private val log = LoggerFactory.getLogger(this.getClass)

  /**
   * 按key获取
   *
   * @param key key
   * @return 可能为None
   */
  def get(key: Array[Byte]): Option[MemTableValue] = this.mvcc match
    case None => getWithTs(key, TS_MAX)
    case Some(mvcc) => mvcc.newTxn(this.clone(), options.serializable, true).get(key)

  def getWithTs(key: Array[Byte], ts: Long): Option[MemTableValue] = {
    assert(key != null && !key.isEmpty, "key cannot be empty")
    val snapshot = state.read(_.copy())

    // TODO 提前结束

    // Memtable 部分的迭代器
    val memTableIters = (snapshot.memTable :: snapshot.immutableMemTables)
      .map(mt => mt.scan(Included(MemTableKey.withBeginTs(key)), Included(MemTableKey.withEndTs(key))))
    val memtableIter = MergeIterator(memTableIters)

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

    /* 合成最终的迭代器
     *                                                          |-> MergeIterator(各个Memtable的 MemTableIterator)
     *                                   |-> TwoMergeIterator ->|
     *                                   |                      |-> MergeIterator(L0各个SST的 SsTableIterator)
     * LsmIterator -> TwoMergeIterator ->|
     *                                   |-> MergeIterator(L1及以上各个Level的 SstConcatIterator )
     */
    val finalIter = LsmIterator(
      TwoMergeIterator(TwoMergeIterator(memtableIter, l0Iter), MergeIterator(levelIters)),
      Unbounded(), ts
    )

    if (finalIter.isValid && finalIter.key().rawKey().equals(key) && !finalIter.deletedValue()) {
      Some(finalIter.value())
    } else {
      None
    }
  }


  def get(key: String): Option[String] = get(key.getBytes).map(new String(_))

  /**
   * 插入或更新
   *
   * @param key   key
   * @param value 如果要执行delete操作，可以传入null
   */
  def put(key: Array[Byte], value: MemTableValue): Unit = if (options.serializable) {
    writeBatch(List(WriteBatchRecord.Put(key, value)))
  } else {
    writeBatch(List(WriteBatchRecord.Put(key, value)))
  }

  def put(key: String, value: String): Unit = put(key.getBytes, value.getBytes)

  /**
   * 按key删除
   *
   * @param key key
   */
  def delete(key: Array[Byte]): Unit = writeBatch(List(WriteBatchRecord.Del(key)))

  def delete(key: String): Unit = delete(key.getBytes)

  def writeBatch(batch: Seq[WriteBatchRecord]): Long = {
    def doWriteBatch(record: WriteBatchRecord, ts: Long): Unit = {
      record match
        case WriteBatchRecord.Del(key: Array[Byte]) =>
          assert(key != null && !key.isEmpty, "key cannot be empty")
          // 这里 MemTable 自己的线程安全由 ConcurrentHashMap 保证，所以只要读锁
          val estimatedSize = state.read(st => {
            st.memTable.put(MemTableKey(key, ts), DELETE_TOMBSTONE)
            st.memTable.approximateSize.get()
          })
          tryFreezeMemTable(estimatedSize)
        case WriteBatchRecord.Put(key: Array[Byte], value: MemTableValue) =>
          assert(key != null && !key.isEmpty, "key cannot be empty")
          assert(value != null, "value cannot be empty")
          // 这里 MemTable 自己的线程安全由 ConcurrentHashMap 保证，所以只要读锁
          val estimatedSize = state.read(st => {
            st.memTable.put(MemTableKey(key, ts), value)
            st.memTable.approximateSize.get()
          })
          tryFreezeMemTable(estimatedSize)
    }

    this.mvcc match
      case None =>
        batch.foreach(doWriteBatch(_, 0))
        0L
      case Some(mvcc) => try {
        // 确保只有一个线程可以操作
        mvcc.writeLock.lock()
        // 获取新时间戳/版本
        val ts = mvcc.latestCommitTs() + 1
        batch.foreach(doWriteBatch(_, ts))
        // 更新时间戳/版本
        mvcc.updateCommitTs(ts)
        ts
      } finally {
        mvcc.writeLock.unlock()
      }
  }

  def addCompactionFilter(compactionFilter: CompactionFilter): Unit = {
    compactionFilters.offer(compactionFilter)
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
      // 极端情况，immutable Memtable个数可能超过限值太多，所以这里改成全部flush了
      // 倒序，从早到新遍历所有已冻结的memtable进行flush操作
      for (flushMemTable <- state.immutableMemTables.reverse) {
        // 构建SST、写入SST文件
        val builder = SsTableBuilder(options.blockSize, SsTableCompressor.create(options.compressorOptions))
        flushMemTable.flush(builder)
        val sstId = flushMemTable.id
        val sst = builder.build(sstId, Some(blockCache), fileOfSst(sstId))
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
      }
      // 移除MemTable、加入到L0 table
      state.immutableMemTables = List()
    } finally {
      state.stateLock.unlock()
    }
  }

  def scan(lower: Bound, upper: Bound): StorageIterator[RawKey] = this.mvcc match
    case None => scanWithTs(lower, upper, TS_RANGE_END)
    case Some(mvcc) => mvcc.newTxn(this.clone(), options.serializable, true).scan(lower, upper)

  def scanWithTs(lower: Bound, upper: Bound, readTs: Long): FusedIterator[RawKey] = {
    val snapshot = state.read(_.copy())
    // MemTable 部分的迭代器
    val memTableIters = (snapshot.memTable :: snapshot.immutableMemTables)
      .map(mt => mt.scan(Bound.withBeginTs(lower), Bound.withEndTs(upper)))
    val memTablesIter = MergeIterator(memTableIters)

    // L0 SST 部分的迭代器
    val ssTableIters = snapshot.l0SsTables.map(snapshot.ssTables(_))
      // 过滤key范围可能包含当前scan范围的sst，减少IO
      .filter(sst => rangeOverlap(lower, upper, sst.firstKey, sst.lastKey))
      .map(sst => SsTableIterator.createByLowerBound(sst, lower))
    val l0ssTablesIter = MergeIterator(ssTableIters)

    // levels SST 部分的迭代器
    val levelIters = snapshot.levels.map((_, levelSstIds) => {
      val levelSsts = levelSstIds
        .map(snapshot.ssTables(_))
        .filter(sst => rangeOverlap(lower, upper, sst.firstKey, sst.lastKey))
      SstConcatIterator.createByLowerBound(levelSsts, lower)
    })
    val levelTablesIter = MergeIterator(levelIters)

    log.debug("{}, {}, {}", memTablesIter.numActiveIterators(), l0ssTablesIter.numActiveIterators(), levelTablesIter.numActiveIterators())
    /* 合成最终的迭代器
     *                                           |-> MergeIterator(各个Memtable的 MemTableIterator)
     *                    |-> TwoMergeIterator --|
     *                    |                      |-> MergeIterator(L0各个SST的 SsTableIterator)
     * TwoMergeIterator ->|
     *                    |-> MergeIterator(L1及以上各个Level的 SstConcatIterator )
     */
    val mergedIter = TwoMergeIterator(TwoMergeIterator(memTablesIter, l0ssTablesIter), levelTablesIter)
    // 再包两层，分别做熔断（异常处理）和 多版本控制+删除墓碑处理
    FusedIterator(LsmIterator(mergedIter, upper, readTs))
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
  def compactGenerateSstFromIter(iter: StorageIterator[MemTableKey], compactToBottomLevel: Boolean): List[SsTable] = {
    import scala.util.control.Breaks.{break, breakable}
    var builder: Option[SsTableBuilder] = None
    val newSstList = new ArrayBuffer[SsTable]()
    // 当前Transaction 的水位，时间戳/版本 超过水位（比水位更新/时间戳更大）的版本需要保留，否则可以删掉
    val watermark = mvcc.map(_.watermark()).getOrElse(MemTableKey.TS_MIN)
    // 记录最近处理的key，同个key写入同个sst，即便超过了sst大小限制，这样方便处理判断key区间
    var lastKey = Array[Byte]()
    // 记录当前是否是Watermark下的第一个key
    var firstKeyBelowWatermark = false
    val compactionFilters = this.compactionFilters.asScala.toList
    while (iter.isValid) breakable {
      if (builder.isEmpty) {
        builder = Some(SsTableBuilder(options.blockSize, SsTableCompressor.create(options.compressorOptions)))
      }

      val sameAsLastKey = iter.key().rawKey().equals(lastKey)
      if (!sameAsLastKey) {
        firstKeyBelowWatermark = true
      }

      // 处理delete墓碑
      if (compactToBottomLevel
        //        && !sameAsLastKey
        && iter.key().ts <= watermark
        && iter.value().sameElements(DELETE_TOMBSTONE)) {
        // 如果压缩合并底部的level，那么水位线以下的delete墓碑不用保留了
        // 如果不是底部 compactToBottomLevel == false，那么不能直接跳过，否则会导致墓碑丢失，要留到最底一层才能删除墓碑
        // 而如果 sameAsLastKey == true, 那么是和上一次遍历是同一个key，而遍历是从更新版本/更大时间戳开始的，说明这个key有更新的版本
        lastKey = iter.key().bytes.clone()
        iter.next()
        firstKeyBelowWatermark = false
        // 这里的break是break掉前面的 breakable，breakable是while里面的，所以实际是继续while， 下同
        break()
      }

      // 跳过同key早于水位线的版本
      if (iter.key().ts <= watermark) {
        if (sameAsLastKey && !firstKeyBelowWatermark) {
          // 跟之前的key一样，但是出现了
          iter.next()
          break()
        }
        firstKeyBelowWatermark = false
        if (compactionFilters.nonEmpty) {
          // 对于满足用户指定的压缩过滤器的过时版本key（小于水位线），直接删除（即跳过，继续下个while）
          for (filter <- compactionFilters) filter match
            case CompactionFilter.Prefix(prefix) =>
              if (iter.key().bytes.startsWith(prefix)) {
                iter.next()
                break()
              }
        }
      }

      var innerBuilder = builder.get
      // builder满了则生成sst，并新开一个builder
      if (innerBuilder.estimateSize() >= options.targetSstSize && !sameAsLastKey) {
        val sstId = nextSstId.incrementAndGet()
        val sst = innerBuilder.build(sstId, Some(blockCache), fileOfSst(sstId))
        newSstList += sst
        innerBuilder = SsTableBuilder(options.blockSize, SsTableCompressor.create(options.compressorOptions))
        builder = Some(innerBuilder)
      }
      innerBuilder.add(iter.key(), iter.value())
      if (!sameAsLastKey) {
        lastKey = iter.key().bytes.clone()
      }
      iter.next()
    }
    // 最后一个SsTableBuilder
    if (builder.isDefined) {
      val sstId = nextSstId.incrementAndGet()
      val sst = builder.get.build(sstId, Some(blockCache), fileOfSst(sstId))
      newSstList += sst
    }
    newSstList.toList
  }

  def newTxn(): Transaction = {
    mvcc.map(_.newTxn(this.clone(), options.serializable)).orNull
  }

  override def clone(): LsmStorageInner = this.copy(state = state.copy())

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
      case Excluded(right: Key) if right.rawKey() <= sstBegin.rawKey() => return false
      case Included(right: Key) if right.rawKey() < sstBegin.rawKey() => return false
      case _ =>
    // 判断scan的左边界如果大于SST的最右边最后一个key，那么这个sst肯定不包含这个scan范围
    userBegin match
      case Excluded(left: Key) if left.rawKey() >= sstEnd.rawKey() => return false
      case Included(left: Key) if left.rawKey() > sstEnd.rawKey() => return false
      case _ =>
    true
  }

  def dumpStorage(): Unit = {
    val itr = scan(Unbounded(), Unbounded())
    print("Storage content: ")
    while (itr.isValid) {
      print(s"${new String(itr.key().bytes)} => ${new String(itr.value())}, ")
      itr.next()
    }
    println()
  }

  def dumpState(): String = state.dumpState()

  def triggerFlush(): Unit = {
    val (memtableNum, numLimit) = state.read(st => (st.immutableMemTables.length, options.numMemTableLimit))
    val needTrigger = memtableNum >= numLimit
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
  case Del(key: Array[Byte]) extends WriteBatchRecord

  case Put(key: Array[Byte], value: MemTableValue) extends WriteBatchRecord

}