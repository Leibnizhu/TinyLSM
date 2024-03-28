package io.github.leibnizhu.tinylsm.mvcc

import io.github.leibnizhu.tinylsm.LsmStorageInner
import io.github.leibnizhu.tinylsm.utils.Mutex

import java.util
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.{Lock, ReentrantLock}

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