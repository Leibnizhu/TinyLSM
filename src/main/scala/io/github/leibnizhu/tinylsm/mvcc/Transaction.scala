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

  def get(key: Array[Byte]): Option[Array[Byte]] = {
    None
  }

  def scan(lower: Bound, upper: Bound): TxnIterator = {
    null
  }

  def put(key: Array[Byte], value: Array[Byte]): Unit = {

  }

  def delete(key: Array[Byte]): Unit = {

  }

  def commit(): Unit = {

  }

  def drop(): Unit = {

  }
}
