package io.github.leibnizhu.tinylsm.mvcc

import io.github.leibnizhu.tinylsm.LsmStorageInner
import io.github.leibnizhu.tinylsm.utils.Mutex

import java.util
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.{Lock, ReentrantLock}

/**
 * 管控MVCC，会有多个 Transaction 同时访问
 *
 * @param writeLock     Transaction 提交、写入MemTable时的锁
 * @param commitLock    Transaction 提交的锁
 * @param ts            记录 Tuple2(当前最新Commit版本, Watermark)
 * @param committedTxns 已提交的 Transaction 信息
 */
class LsmMvccInner(
                    val writeLock: Lock = new ReentrantLock(),
                    val commitLock: Lock = new ReentrantLock(),
                    val ts: Mutex[(Long, Watermark)],
                    val committedTxns: Mutex[util.TreeMap[Long, CommittedTxnData]] = new Mutex(new util.TreeMap())
                  ) {

  def latestCommitTs(): Long = {
    ts.execute(_._1)
  }

  def updateCommitTs(newTs: Long): Unit = {
    ts.update(_.copy(_1 = newTs))
  }

  def watermark(): Long = {
    // 如果没有活动中的Transaction，那么水位应该是最后提交的版本
    ts.execute((ts, wm) => wm.watermark().getOrElse(ts))
  }

  def newTxn(inner: LsmStorageInner, serializable: Boolean): Transaction = ts.execute((readTs, watermark) => {
    watermark.addReader(readTs)
    Transaction(
      readTs = readTs,
      inner = inner,
      localStorage = new ConcurrentSkipListMap(),
      committed = new AtomicBoolean(false),
      keyHashes = if (serializable) Some(Mutex((new util.HashSet[Int](), new util.HashSet[Int]()))) else None
    )
  })
}

object LsmMvccInner {
  def apply(lastCommitTs: Long): LsmMvccInner =
    new LsmMvccInner(ts = Mutex((lastCommitTs, new Watermark)))
}