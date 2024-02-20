package io.github.leibnizhu.tinylsm

import java.io.File
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.{Lock, ReadWriteLock, ReentrantLock, ReentrantReadWriteLock}
import scala.util.boundary

class LsmStorageState(
                       // 当前Memtable
                       var memTable: MemTable,
                       // 不可变的Memtable，从最近到最早的
                       var immutableMemTables: List[MemTable]) {
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
}

object LsmStorageState {
  def apply(options: LsmStorageOptions): LsmStorageState = {
    new LsmStorageState(MemTable(0), List[MemTable]())
  }
}

private[tinylsm] class LsmStorageInner(
                                        // wal的根目录
                                        path: File,
                                        val state: LsmStorageState,
                                        options: LsmStorageOptions,
                                        nextSstId: AtomicInteger) {
  /**
   * 按key获取
   *
   * @param key key
   * @return 可能为None
   */
  def get(key: MemTableKey): Option[MemTableValue] = {
    assert(key != null && !key.isEmpty, "key cannot be empty")

    // 先判断当前未 freeze 的MemTable是否有需要读取的值
    val inMemTable = state.read(_.memTable.get(key))
    if (inMemTable.isDefined) {
      // 如果读取出来是墓碑（空Array）需要过滤返回None
      return inMemTable.filter(!_.sameElements(LsmStorageInner.DELETE_TOMBSTONE))
    }

    // 由新到旧遍历已 freeze 的MemTable，找到直接返回
    state.read(st => {
      boundary:
        for mt <- st.immutableMemTables do
          val curValue = mt.get(key)
          if (curValue.isDefined) {
            // 如果读取出来是墓碑（空Array）需要过滤返回None
            boundary.break(curValue.filter(!_.sameElements(LsmStorageInner.DELETE_TOMBSTONE)))
          }
        None
    })
  }

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

  /**
   * 按key删除
   *
   * @param key key
   */
  def delete(key: MemTableKey): Unit = {
    put(key, LsmStorageInner.DELETE_TOMBSTONE)
  }

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
    val newMemTable = if (options.enableWal) MemTable(newMemTableId, Some(pathOfWal(newMemTableId))) else MemTable(newMemTableId)
    freezeMemTableWithMemTable(newMemTable)
    // TODO 文件操作
  }

  private def freezeMemTableWithMemTable(newMemTable: MemTable): Unit = {
    state.write(st => {
      st.immutableMemTables = st.memTable :: st.immutableMemTables
      st.memTable = newMemTable
    })
  }

  private def pathOfWal(sstId: Int): File = {
    new File(path, sstId.toString)
  }
}

object LsmStorageInner {
  def apply(path: File, options: LsmStorageOptions): LsmStorageInner = {
    val state = LsmStorageState(options)
    new LsmStorageInner(path, state, options, AtomicInteger(0))
  }

  private val DELETE_TOMBSTONE = Array[Byte]()

}

class TinyLsm {

}

case class LsmStorageOptions
(
  // Block大小，单位是 bytes
  blockSize: Int,
  // SST大小，单位是 bytes, 同时也是MemTable容量限制的近似值
  targetSstSize: Int,
  // MemTable在内存中的最大占用空间, 到达这个大小后会 flush 到 L0
  numMemTableLimit: Int,
  // Compaction配置
  compactionOptions: CompactionOptions,
  // 是否启用WAL
  enableWal: Boolean,
  // 是否可序列化
  serializable: Boolean
)