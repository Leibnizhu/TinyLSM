package io.github.leibnizhu.tinylsm

import io.github.leibnizhu.tinylsm.block.BlockCache
import io.github.leibnizhu.tinylsm.compact.CompactionOptions.{FullCompaction, LeveledCompactionOptions, SimpleCompactionOptions, TieredCompactionOptions}
import io.github.leibnizhu.tinylsm.compact.{CompactionController, CompactionOptions, FullCompactionTask}
import io.github.leibnizhu.tinylsm.iterator.*
import io.github.leibnizhu.tinylsm.utils.*
import org.slf4j.LoggerFactory

import java.io.File
import java.util
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.{Lock, ReadWriteLock, ReentrantLock, ReentrantReadWriteLock}
import java.util.{Arrays, Timer}
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.boundary

/**
 * 全是var，var里面是immutable的，需要snapshot的话直接copy这个state就可以
 * state.read(_.copy())
 */
case class LsmStorageState(
                            // 当前Memtable
                            var memTable: MemTable,
                            // 不可变的Memtable，从最近到最早的
                            var immutableMemTables: List[MemTable],
                            // L0 SST 从最新到最早的
                            var l0SsTables: List[Int],
                            // 按key 范围排序的SsTable， L1 -> Lmax，每层是 (L几，List(包含的SST ID))
                            var levels: List[Level] = List((1, List())),
                            // SST 对象
                            var ssTables: Map[Int, SsTable] = Map()
                          ) {
  // 对 MemTable 做 freeze 操作的读写锁
  val rwLock: ReadWriteLock = ReentrantReadWriteLock()
  // MemTable 做 freeze 时，保证只有一个线程执行 freeze 的锁
  val stateLock: Lock = ReentrantLock()

  /**
   * 封装只读操作
   *
   * @param f 只读的操作
   * @tparam T 只读的响应类型
   * @return 只读的结果
   */
  def read[T](f: LsmStorageState => T): T = {
    val readLock = rwLock.readLock()
    try {
      readLock.lock()
      f(this)
    } finally {
      readLock.unlock()
    }
  }

  /**
   * 封装写操作
   *
   * @param f 写的操作
   * @tparam T 写操作的响应类型
   * @return 写的结果
   */
  def write[T](f: LsmStorageState => T): T = {
    val writeLock = rwLock.writeLock()
    try {
      writeLock.lock()
      f(this)
    } finally {
      writeLock.unlock()
    }
  }

  def dumpState(): Unit = {
    val snapshot = this.read(_.copy())
    println(s"Current MemTable: ${snapshot.memTable.id}")
    println(s"Frozen MemTables: [${snapshot.immutableMemTables.map(_.id).mkString(", ")}]")
    println(s"L0\t(${snapshot.l0SsTables.length}): [${snapshot.l0SsTables.mkString(", ")}]")
    for ((level, files) <- snapshot.levels) {
      println(s"L$level\t(${files.length}): [${files.mkString(", ")}]")
    }
    println(s"SST: {${snapshot.ssTables.keys.mkString(", ")}}")
  }
}

object LsmStorageState {
  def apply(options: LsmStorageOptions): LsmStorageState = {
    val levels = options.compactionOptions match
      case SimpleCompactionOptions(_, _, maxLevels) => makeLevelsByMax(maxLevels)
      case LeveledCompactionOptions(_, _, maxLevels, _) => makeLevelsByMax(maxLevels)
      case FullCompaction => makeLevelsByMax(1)
      case t: TieredCompactionOptions => List()
      case _ => List()
    new LsmStorageState(MemTable(0), List[MemTable](), List(), levels)
  }

  private def makeLevelsByMax(maxLevels: Int): List[Level] =
    (1 to maxLevels).map((_, List[Int]())).toList
}

private[tinylsm] class LsmStorageInner(
                                        // wal的根目录
                                        path: File,
                                        val state: LsmStorageState,
                                        val blockCache: BlockCache,
                                        val options: LsmStorageOptions,
                                        nextSstId: AtomicInteger,
                                        val compactionController: CompactionController) {
  private val log = LoggerFactory.getLogger(classOf[LsmStorageInner])

  /**
   * 按key获取
   *
   * @param key key
   * @return 可能为None
   */
  def get(key: MemTableKey): Option[MemTableValue] = {
    assert(key != null && !key.isEmpty, "key cannot be empty")

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
    if (sstIter.isValid && sstIter.key().sameElements(key) && !sstIter.value().sameElements(DELETE_TOMBSTONE)) {
      // l0 sst 有效、有当前查询的key、且值不为空，即找到了value
      return Some(sstIter.value())
    }
    None
  }


  def get(key: String): Option[String] = get(key.getBytes).map(new String(_))

  /**
   * 插入或更新
   *
   * @param key   key
   * @param value 如果要执行delete操作，可以传入null
   */
  def put(key: MemTableKey, value: MemTableValue): Unit = {
    assert(key != null && !key.isEmpty, "key cannot be empty")
    assert(value != null, "value cannot be empty")
    doPut(key, value)
  }

  def put(key: String, value: String): Unit = put(key.getBytes, value.getBytes)

  /**
   * 按key删除
   *
   * @param key key
   */
  def delete(key: MemTableKey): Unit = put(key, DELETE_TOMBSTONE)

  def delete(key: String): Unit = delete(key.getBytes)

  private def doPut(key: MemTableKey, value: MemTableValue): Unit = {
    // 这里 MemTable 自己的线程安全由 ConcurrentHashMap 保证，所以只要读锁
    val estimatedSize = state.read(st => {
      st.memTable.put(key, value)
      st.memTable.approximateSize.get()
    })
    tryFreezeMemTable(estimatedSize)
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
    val newMemTableId = nextSstId.incrementAndGet()
    val newMemTable = if (options.enableWal) MemTable(newMemTableId, Some(fileOfWal(newMemTableId))) else MemTable(newMemTableId)
    freezeMemTableWithMemTable(newMemTable)
    // TODO 文件操作
  }

  private def freezeMemTableWithMemTable(newMemTable: MemTable): Unit = {
    state.write(st => {
      st.immutableMemTables = st.memTable :: st.immutableMemTables
      st.memTable = newMemTable
    })
  }

  private def fileOfWal(sstId: Int): File = {
    new File(path, "%05d.wal".format(sstId))
  }

  private def fileOfSst(sstId: Int): File = {
    new File(path, "%05d.sst".format(sstId))
  }

  /**
   * 强制将最早的一个ImmutableMemTable 刷入磁盘
   */
  def forceFlushNextImmutableMemTable(): Unit = {
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
        fileOfWal(sstId).delete()
      }
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
    }).toList
    val levelTablesIter = MergeIterator(levelIters)
    val mergedIter = TwoMergeIterator(TwoMergeIterator(memTablesIter, l0ssTablesIter), levelTablesIter)
    FusedIterator(LsmIterator(mergedIter, upper))
  }


  def forceFullCompaction(): Unit = {
    if (options.compactionOptions == CompactionOptions.NoCompaction) {
      throw new IllegalStateException("full compaction can only be called with compaction is enabled")
    }

    // 执行 full compaction
    val snapshot = this.state.read(_.copy())
    val l0SsTables = List(snapshot.l0SsTables: _*)
    val l1SsTables = if (snapshot.levels.isEmpty) List() else List(snapshot.levels.head._2: _*)
    val compactionTask = FullCompactionTask(l0SsTables, l1SsTables)
    log.info("Force full compaction: {}", compactionTask)
    val newSsTables = compactionTask.doCompact(this)
    val newSstIds = newSsTables.map(_.sstId())
    val sstToRemove = compactionController.applyCompactionToState(state, newSsTables, compactionTask)
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
      case Excluded(r: MemTableKey) if util.Arrays.compare(r, sstBegin) <= 0 => return false
      case Included(r: MemTableKey) if util.Arrays.compare(r, sstBegin) < 0 => return false
      case _ => {}
    // 判断scan的左边界如果大于SST的最右边最后一个key，那么这个sst肯定不包含这个scan范围
    userBegin match
      case Excluded(r: MemTableKey) if util.Arrays.compare(r, sstEnd) >= 0 => return false
      case Included(r: MemTableKey) if util.Arrays.compare(r, sstEnd) > 0 => return false
      case _ => {}
    true
  }

  def dumpStorage(): Unit = {
    val itr = scan(Unbounded(), Unbounded())
    print("Storage content: ")
    while (itr.isValid) {
      print(s"${new String(itr.key())} => ${new String(itr.value())}, ")
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
    // 释放锁之后再做文件IO
    deleteSstFiles(sstToRemove)
  }

  private def deleteSstFiles(sstIds: List[Int]): Unit = {
    for (sstId <- sstIds) {
      val sstFile = fileOfSst(sstId)
      val deleted = sstFile.delete()
      log.info("Deleted SST table file: {} {}", sstFile.getName, if (deleted) "success" else "failed")
    }
  }
}

object LsmStorageInner {
  private val log = LoggerFactory.getLogger(classOf[LsmStorageInner])

  def apply(path: File, options: LsmStorageOptions): LsmStorageInner = {
    val state = LsmStorageState(options)
    val blockCache = BlockCache.apply(128)
    val compactionController = CompactionController(options.compactionOptions)
    log.info("Start LsmStorageInner with lsm dir: {}", path.getAbsolutePath)
    new LsmStorageInner(path, state, blockCache, options, AtomicInteger(0), compactionController)
  }

}

class TinyLsm(val inner: LsmStorageInner) {
  private val flushThread = spawnFlushThread()
  private val compactionThread = spawnCompactionThread()

  def get(key: MemTableKey): Option[MemTableValue] = inner.get(key)

  def get(key: String): Option[String] = inner.get(key)

  def put(key: MemTableKey, value: MemTableValue): Unit = inner.put(key, value)

  def put(key: String, value: String): Unit = inner.put(key, value)

  def delete(key: MemTableKey): Unit = inner.delete(key)

  def delete(key: String): Unit = inner.delete(key)

  def scan(lower: Bound, upper: Bound): FusedIterator[MemTableKey, MemTableValue] = inner.scan(lower, upper)

  def newTxn(): Unit = inner.newTxn()

  def forceFullCompaction(): Unit = inner.forceFullCompaction()

  def spawnFlushThread(): Timer = {
    val timer = new Timer()
    timer.schedule(() => inner.triggerFlush(), 0, 50)
    timer
  }

  def spawnCompactionThread(): Timer = {
    val timer = new Timer()
    timer.schedule(() => inner.triggerCompact(), 0, 50)
    timer
  }

  def close(): Unit = {
    flushThread.cancel()
    compactionThread.cancel()
    if (inner.options.enableWal) {
      // TODO 同步wal目录
    }
    while (!inner.state.read(st => st.immutableMemTables.nonEmpty)) {
      inner.forceFlushNextImmutableMemTable()
    }
    //TODO sst目录
  }
}

object TinyLsm {
  def apply(path: File, options: LsmStorageOptions): TinyLsm = {
    new TinyLsm(LsmStorageInner(path, options))
  }

}

case class LsmStorageOptions
(
  // Block大小，单位是 bytes，应该小于或等于这个值
  blockSize: Int,
  // SST大小，单位是 bytes, 同时也是MemTable容量限制的近似值
  targetSstSize: Int,
  // MemTable在内存中的最多个数, 超过这么多MemTable后会 flush 到 L0
  numMemTableLimit: Int,
  // Compaction配置
  compactionOptions: CompactionOptions,
  // 是否启用WAL
  enableWal: Boolean,
  // 是否可序列化
  serializable: Boolean
)

object LsmStorageOptions {
  def defaultOption(): LsmStorageOptions = LsmStorageOptions(
    4096,
    2 << 20,
    50,
    CompactionOptions.NoCompaction,
    false,
    false)

  def fromConfig(): LsmStorageOptions = LsmStorageOptions(
    Config.BlockSize.getInt,
    Config.TargetSstSize.getInt,
    Config.MemTableLimitNum.getInt,
    // TODO
    CompactionOptions.NoCompaction,
    Config.EnableWal.getBoolean,
    Config.Serializable.getBoolean
  )
}