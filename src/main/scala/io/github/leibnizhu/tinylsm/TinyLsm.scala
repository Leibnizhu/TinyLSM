package io.github.leibnizhu.tinylsm

import io.github.leibnizhu.tinylsm.iterator.StorageIterator
import io.github.leibnizhu.tinylsm.mvcc.Transaction
import io.github.leibnizhu.tinylsm.utils.Bound
import org.slf4j.LoggerFactory

import java.io.File
import java.util.Timer

object TinyLsm {
  def apply(path: File, options: LsmStorageOptions): TinyLsm = {
    new TinyLsm(LsmStorageInner(path, options))
  }

}

class TinyLsm(val inner: LsmStorageInner) {
  private val log = LoggerFactory.getLogger(this.getClass)
  private val flushThread = spawnFlushThread()
  private val compactionThread = spawnCompactionThread()
  private val manifestCompactionThread = spawnManifestCompactionThread()

  def get(key: Array[Byte]): Option[MemTableValue] = inner.get(key)

  def get(key: String): Option[String] = inner.get(key)

  def put(key: Array[Byte], value: MemTableValue): Unit = inner.put(key, value)

  def put(key: String, value: String): Unit = inner.put(key, value)

  def delete(key: Array[Byte]): Unit = inner.delete(key)

  def delete(key: String): Unit = inner.delete(key)

  def scan(lower: Bound, upper: Bound): StorageIterator[RawKey] = inner.scan(lower, upper)

  def newTxn(): Transaction = inner.newTxn()

  def writeBatch(batch: Seq[WriteBatchRecord]): Long = inner.writeBatch(batch)

  def forceFlush(): Unit = {
    if (inner.state.read(!_.memTable.isEmpty)) {
      inner.forceFreezeMemTable()
    }
    if (inner.state.read(_.immutableMemTables.nonEmpty)) {
      inner.forceFlushNextImmutableMemTable()
    }
  }

  def forceFullCompaction(): Unit = inner.forceFullCompaction()

  private def spawnFlushThread(): Timer = {
    val timer = new Timer()
    timer.schedule(() => inner.triggerFlush(), 0, 50)
    timer
  }

  private def spawnCompactionThread(): Timer = {
    val timer = new Timer()
    timer.schedule(() => inner.triggerCompact(), 0, 50)
    timer
  }

  private def spawnManifestCompactionThread(): Timer = {
    val timer = new Timer()
    timer.schedule(() => inner.triggerManifestCompact(), 0, 100)
    timer
  }

  def close(): Unit = {
    flushThread.cancel()
    compactionThread.cancel()
    // 开了wal的话只要确保Memtable写入WAL即可
    if (inner.options.enableWal) {
      inner.syncWal()
      return
    }
    // 没开wal的话需要把MemTable写入sst
    if (inner.state.read(_.memTable.nonEmpty)) {
      inner.freezeMemTableWithMemTable(MemTable(inner.nextSstId.get()))
    }
    while (inner.state.read(st => st.immutableMemTables.nonEmpty)) {
      log.info("Still {} frozen MemTables is not flushed", inner.state.read(st => st.immutableMemTables.length))
      inner.forceFlushNextImmutableMemTable()
    }
  }
}