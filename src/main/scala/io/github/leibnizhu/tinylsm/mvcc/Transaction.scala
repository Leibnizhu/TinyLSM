package io.github.leibnizhu.tinylsm.mvcc

import io.github.leibnizhu.tinylsm.iterator.TwoMergeIterator
import io.github.leibnizhu.tinylsm.utils.{Bound, Mutex}
import io.github.leibnizhu.tinylsm.{DELETE_TOMBSTONE, LsmStorageInner, RawKey, WriteBatchRecord}
import org.slf4j.LoggerFactory

import java.util
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicBoolean
import scala.jdk.CollectionConverters.*

case class Transaction(
                        var readTs: Long,
                        inner: LsmStorageInner,
                        // Transaction 内的k-v对，由于没提交，所以不用记录版本，key只要放 字节数组
                        // TODO 体积过大时flush到磁盘
                        localStorage: ConcurrentSkipListMap[RawKey, Array[Byte]],
                        committed: AtomicBoolean,
                        /// 分别是 写 和 读 的key的hash集合
                        keyHashes: Option[Mutex[(util.HashSet[Int], util.HashSet[Int])]],
                      ) {
  private val log = LoggerFactory.getLogger(this.getClass)

  def get(key: String): Option[String] = get(key.getBytes).map(new String(_))

  def get(bytes: Array[Byte]): Option[Array[Byte]] = {
    if (committed.get()) {
      throw new IllegalStateException("cannot operate on committed Transaction!")
    }

    val key = RawKey(bytes)
    if (keyHashes.isDefined) {
      keyHashes.get.execute((_, readHashes) => readHashes.add(key.keyHash()))
    }

    if (localStorage.containsKey(key)) {
      // Transaction 内部缓存能命中
      val value = localStorage.get(key)
      if (value.sameElements(DELETE_TOMBSTONE)) {
        None
      } else {
        Some(value)
      }
    } else {
      // 从开启 Transaction 之前原来的 LsmStorageInner 中读取
      inner.getWithTs(bytes, readTs)
    }
  }

  def scan(lower: Bound, upper: Bound): TxnIterator = {
    if (committed.get()) {
      throw new IllegalStateException("cannot operate on committed Transaction!")
    }
    val fuseIter = inner.scanWithTs(lower, upper, readTs)
    val localIter = TxnLocalIterator(localStorage, lower, upper)
    TxnIterator(this.copy(), TwoMergeIterator(localIter, fuseIter))
  }

  def put(key: String, value: String): Unit = put(key.getBytes, value.getBytes)

  def put(bytes: Array[Byte], value: Array[Byte]): Unit = {
    if (committed.get()) {
      throw new IllegalStateException("cannot operate on committed Transaction!")
    }

    val key = RawKey(bytes)
    localStorage.put(key, value)
    keyHashes.foreach(_.execute((writeHashes, _) => writeHashes.add(key.keyHash())))
  }

  def delete(key: String): Unit = delete(key.getBytes)

  def delete(bytes: Array[Byte]): Unit = {
    if (committed.get()) {
      throw new IllegalStateException("cannot operate on committed Transaction!")
    }

    val key = RawKey(bytes)
    localStorage.put(key, DELETE_TOMBSTONE)
    keyHashes.foreach(_.execute((writeHashes, _) => writeHashes.add(key.keyHash())))
  }

  def commit(): Unit = {
    if (!committed.compareAndSet(false, true)) {
      throw new IllegalStateException("cannot operate on committed Transaction!")
    }
    // 本地缓存转成batch操作
    val batch = localStorage.entrySet().iterator().asScala.map(entry => entry.getValue match
      case DELETE_TOMBSTONE => WriteBatchRecord.Del(entry.getKey.bytes)
      case _ => WriteBatchRecord.Put(entry.getKey.bytes, entry.getValue)
    ).toSeq
    val ts = inner.writeBatch(batch)
    // TODO
    log.info("Transaction committed {} records, readTs:{}, new ts: {}", localStorage.size(), readTs, ts)
  }

  def rollback(): Unit = {
    this.inner.mvcc.foreach(_.ts.execute((_, watermark) => watermark.removeReader(this.readTs)))
  }
}
