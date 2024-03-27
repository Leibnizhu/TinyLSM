package io.github.leibnizhu.tinylsm.mvcc

import io.github.leibnizhu.tinylsm.LsmStorageInner
import io.github.leibnizhu.tinylsm.utils.{Bound, Mutex}

import java.util
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicBoolean

case class Transaction(
                        var readTs: Long,
                        inner: LsmStorageInner,
                        localStorage: ConcurrentSkipListMap[Array[Byte], Array[Byte]],
                        committed: AtomicBoolean,
                        /// Write set and read set
                        keyHashes: Option[Mutex[(util.HashSet[Int], util.HashSet[Int])]],
                      ) {

  def get(key: String): Option[String] = get(key.getBytes).map(new String(_))

  def get(key: Array[Byte]): Option[Array[Byte]] = {
    if (committed.get()) {
      throw new IllegalStateException("cannot operate on committed Transaction!")
    }

    inner.getWithTs(key, readTs)
  }

  def scan(lower: Bound, upper: Bound): TxnIterator = {
    if (committed.get()) {
      throw new IllegalStateException("cannot operate on committed Transaction!")
    }
    val fuseIter = inner.scanWithTs(lower, upper, readTs)
    TxnIterator(this.copy(), fuseIter)
  }

  def put(key: String, value: String): Unit = put(key.getBytes, value.getBytes)

  def put(key: Array[Byte], value: Array[Byte]): Unit = {
    if (committed.get()) {
      throw new IllegalStateException("cannot operate on committed Transaction!")
    }

    // TODO
  }

  def delete(key: String): Unit = delete(key.getBytes)

  def delete(key: Array[Byte]): Unit = {
    if (committed.get()) {
      throw new IllegalStateException("cannot operate on committed Transaction!")
    }

    // TODO
  }

  def commit(): Unit = {
    // TODO
  }

  def drop(): Unit = {
    // TODO
  }
}
