package io.github.leibnizhu.tinylsm.compact

import io.github.leibnizhu.tinylsm.iterator.{MergeIterator, SsTableIterator, TwoMergeIterator}
import io.github.leibnizhu.tinylsm.{LsmStorageInner, SsTable}

case class FullCompactionTask(l0SsTableIds: List[Int], l1SsTableIds: List[Int]) extends CompactionTask {
  override def doCompact(storage: LsmStorageInner): List[SsTable] = {
    val ssTableMap = storage.state.ssTables
    val l0Iters = l0SsTableIds.map(ssTableMap(_)).map(SsTableIterator.createAndSeekToFirst)
    val l1Iters = l1SsTableIds.map(ssTableMap(_)).map(SsTableIterator.createAndSeekToFirst)
    val l1SsTables = l1SsTableIds.map(ssTableMap(_))
    val iter = TwoMergeIterator[MergeIterator[SsTableIterator], MergeIterator[SsTableIterator]](
      MergeIterator(l0Iters), MergeIterator(l1Iters)
      //SstConcatIterator.createAndSeekToFirst(l1SsTables)
    )
    storage.compactGenerateSstFromIter(iter, compactToBottomLevel())
  }

  override def compactToBottomLevel(): Boolean = true
}
