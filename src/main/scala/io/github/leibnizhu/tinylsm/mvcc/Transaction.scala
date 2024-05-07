package io.github.leibnizhu.tinylsm.mvcc

import io.github.leibnizhu.tinylsm.iterator.TwoMergeIterator
import io.github.leibnizhu.tinylsm.utils.{Bound, Included, Mutex}
import io.github.leibnizhu.tinylsm.{DELETE_TOMBSTONE, Key, LsmStorageInner, MemTableKey, RawKey, WriteBatchRecord}
import org.slf4j.LoggerFactory

import java.util
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.jdk.CollectionConverters.*

case class Transaction(
                        tid: Int = Transaction.ids.getAndIncrement(),
                        var readTs: Long,
                        inner: LsmStorageInner,
                        // Transaction 内的k-v对，由于没提交，所以不用记录版本，key只要放 字节数组
                        // TODO 体积过大时flush到磁盘
                        localStorage: ConcurrentSkipListMap[RawKey, Array[Byte]],
                        committed: AtomicBoolean,
                        /// 用于可串行化snapshot隔离，分别是 写 和 读 的key的hash集合
                        keyHashes: Option[Mutex[(util.HashSet[Int], util.HashSet[Int])]],
                        // 是否只读一次，如果是，则在执行 get/scan 之后马上关闭
                        readOnce: Boolean,
                      ) {
  private val log = LoggerFactory.getLogger(this.getClass)
  if (!readOnce) {
    log.info("Started new Transaction, ID= {}", tid)
  }

  def get(key: String): Option[String] = get(key.getBytes).map(new String(_))

  def get(bytes: Array[Byte]): Option[Array[Byte]] = {
    if (committed.get()) {
      throw new IllegalStateException(s"cannot operate on committed Transaction(ID=$tid)!")
    }

    val key = RawKey(bytes)
    if (keyHashes.isDefined) {
      keyHashes.get.execute((_, readHashes) => readHashes.add(key.keyHash()))
    }

    val value = if (localStorage.containsKey(key)) {
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
    if (readOnce) {
      rollback()
    }
    value
  }

  def scan(lower: Bound, upper: Bound): TxnIterator = {
    if (committed.get()) {
      throw new IllegalStateException(s"cannot operate on committed Transaction(ID=$tid)!")
    }
    val fuseIter = inner.scanWithTs(lower, upper, readTs)
    val localIter = TxnLocalIterator(localStorage, lower, upper)
    val iterator = TxnIterator(this.copy(), TwoMergeIterator(localIter, fuseIter))
    if (readOnce) {
      rollback()
    }
    iterator
  }

  def prefix(prefix: Key): TxnIterator = {
    if (committed.get()) {
      throw new IllegalStateException(s"cannot operate on committed Transaction(ID=$tid)!")
    }
    val fuseIter = inner.prefixWithTs(prefix, readTs)
    val (lower, upper) = prefix.prefixRange()
    val localIter = TxnLocalIterator(localStorage, lower, upper)
    val iterator = TxnIterator(this.copy(), TwoMergeIterator(localIter, fuseIter))
    if (readOnce) {
      rollback()
    }
    iterator
  }

  def put(key: String, value: String): Unit = put(key.getBytes, value.getBytes)

  def put(bytes: Array[Byte], value: Array[Byte]): Unit = {
    if (committed.get()) {
      throw new IllegalStateException(s"cannot operate on committed Transaction(ID=$tid)!")
    }

    val key = RawKey(bytes)
    localStorage.put(key, value)
    keyHashes.foreach(_.execute((writeHashes, _) => writeHashes.add(key.keyHash())))
  }

  def delete(key: String): Unit = delete(key.getBytes)

  def delete(bytes: Array[Byte]): Unit = {
    if (committed.get()) {
      throw new IllegalStateException(s"cannot operate on committed Transaction(ID=$tid)!")
    }

    val key = RawKey(bytes)
    localStorage.put(key, DELETE_TOMBSTONE)
    keyHashes.foreach(_.execute((writeHashes, _) => writeHashes.add(key.keyHash())))
  }

  def commit(): Unit = {
    if (!committed.compareAndSet(false, true)) {
      throw new IllegalStateException(s"cannot operate on committed Transaction(ID=$tid)!")
    }

    // 检查在其预期提交时间戳之前和其读取时间戳之后启动的所有事务
    val serializability = keyHashes match
      case None => false
      case Some(hashes) =>
        hashes.execute((writeHashes, readHashes) => {
          log.info("Committing transaction(ID={}), write set: {}, head set: {}", tid, writeHashes, readHashes)
          // writeHashes 为空则为只读事务，无需检查
          if (!writeHashes.isEmpty && inner.mvcc.isDefined) {
            // 如果当前Transaction 有写入操作，那么需要判断读取的key 是否有其他 readTs 之后启动并已提交的 Transaction 修改
            inner.mvcc.get.committedTxns.execute(committedTxns => {
              // 遍历 readTs 之后提交的所有事务
              committedTxns.tailMap(this.readTs, false).asScala.foreach((_, txnData) => {
                // 求当前遍历的已提交事务所涉及的key、与当前要提交的事务的读取的key是否有交集，因为事务写入的内容可能是依赖了事务中读取的key
                val intersection = new util.HashSet[Int](txnData.keyHashes)
                intersection.retainAll(readHashes)
                if (!intersection.isEmpty) {
                  val errMsg = s"Serializable check failed, current Transaction(ID=$tid) and " +
                    s"committed Transaction(ID=${txnData.tid}) has conflicts. Key hashes intersection: $intersection."
                  log.error(errMsg)
                  throw new IllegalStateException(errMsg)
                }
              })
            })
          }
        })
        true

    // 本地缓存转成batch操作
    val batch = localStorage.entrySet().iterator().asScala.map(entry => entry.getValue match
      case DELETE_TOMBSTONE => WriteBatchRecord.Del(entry.getKey.bytes)
      case _ => WriteBatchRecord.Put(entry.getKey.bytes, entry.getValue)
    ).toSeq
    val ts = inner.writeBatch(batch)

    // 保存提交信息
    if (serializability && keyHashes.isDefined && inner.mvcc.isDefined) {
      inner.mvcc.get.committedTxns.execute(committedTxns => {
        this.keyHashes.get.execute((writeHashes, _) => {
          val oldData = committedTxns.put(ts, CommittedTxnData(tid, writeHashes, readTs, ts))
          assert(oldData == null, s"Commit ts=$ts's CommittedTxnData should not be existed!")
          // 删除 committedTxns 中多余的旧提交信息，及在水位线以下的事务
          val committedItr = committedTxns.entrySet().iterator()
          val watermark = inner.mvcc.get.watermark()
          import scala.util.control.Breaks.{break, breakable}
          while (committedItr.hasNext) breakable {
            val curCommitted = committedItr.next()
            if (curCommitted.getKey < watermark) {
              committedItr.remove()
              log.debug(s"Removed Transaction(ID=${curCommitted.getValue.tid}, ts=${curCommitted.getKey})")
            } else {
              break()
            }
          }
        })
      })
    }

    log.info("Transaction(ID={}) committed {} records, readTs: {}, new ts: {}", tid, localStorage.size(), readTs, ts)
  }

  def isCommited: Boolean = committed.get()

  def rollback(): Unit = {
    this.inner.mvcc.foreach(_.ts.execute((_, watermark) => watermark.removeReader(this.readTs)))
    if (!readOnce) {
      log.info("Rollback Transaction, ID= {}", tid)
    }
  }
}

object Transaction {
  private val ids = new AtomicInteger(0)
}