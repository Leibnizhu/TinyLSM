package io.github.leibnizhu.tinylsm.mvcc

import io.github.leibnizhu.tinylsm.LsmStorageInner
import io.github.leibnizhu.tinylsm.utils.Mutex

import java.util
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
    ts.execute(_._2.watermark().getOrElse(0))
  }

  def newTxn(inner: LsmStorageInner, serializable: Boolean): Transaction = ???
}

object LsmMvccInner {
  def apply(lastCommitTs: Long): LsmMvccInner =
    new LsmMvccInner(ts = new Mutex((lastCommitTs, new Watermark())))
}