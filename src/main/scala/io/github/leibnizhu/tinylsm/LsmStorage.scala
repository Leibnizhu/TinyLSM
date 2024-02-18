package io.github.leibnizhu.tinylsm

import java.io.File
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.{ReadWriteLock, ReentrantReadWriteLock}
import scala.util.boundary

class LsmStorageState(
                       //当前Memtable
                       var memTable: MemTable,
                       // 不可变的Memtable，从最近到最早的
                       var immutableMemTables: List[MemTable]) {
  val rwLock: ReadWriteLock = ReentrantReadWriteLock()

  def read[T](f: LsmStorageState => T): T = {
    val readLock = rwLock.readLock()
    try {
      readLock.lock()
      f(this)
    } finally {
      readLock.unlock()
    }
  }

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

private[tinylsm] class LsmStorageInner(path: File,
                                       val state: LsmStorageState,
                                       options: LsmStorageOptions,
                                       nextSstId: AtomicInteger) {
  /**
   * 按key获取
   *
   * @param key key
   * @return 可能为None
   */
  def get(key: Array[Byte]): Option[Array[Byte]] = {
    assert(key != null && !key.isEmpty, "key cannot be empty")
    val inMemTable = state.read(_.memTable.get(key))
    if (inMemTable.isDefined) {
      return inMemTable.filter(!_.sameElements(LsmStorageInner.DELETE_TOMBSTONE))
    }
    state.read(st => {
      boundary:
        for mt <- st.immutableMemTables do
          val curValue = mt.get(key)
          if (curValue.isDefined) {
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
  def put(key: Array[Byte], value: Array[Byte]): Unit = {
    assert(key != null && !key.isEmpty, "key cannot be empty")
    assert(value != null, "value cannot be empty")
    doPut(key, value)
  }

  /**
   * 按key删除
   *
   * @param key key
   */
  def delete(key: Array[Byte]): Unit = {
    put(key, LsmStorageInner.DELETE_TOMBSTONE)
  }

  private def doPut(key: Array[Byte], value: Array[Byte]): Unit = {
    val estimatedSize = state.read(st => {
      st.memTable.put(key, value)
      st.memTable.approximateSize.get()
    })
    tryFreezeMemTable(estimatedSize)
  }

  private def tryFreezeMemTable(estimatedSize: Int): Unit = {
    if (estimatedSize >= options.targetSstSize) {
      // 双重校验
      val canFreeze = state.read(st => st.memTable.approximateSize.get() >= this.options.targetSstSize)
      if (canFreeze) {
        this.forceFreezeMemTable()
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
  //是否启用WAL
  enableWal: Boolean,
  // 是否可序列化
  serializable: Boolean
)