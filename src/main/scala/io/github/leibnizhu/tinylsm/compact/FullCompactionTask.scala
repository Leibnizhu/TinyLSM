package io.github.leibnizhu.tinylsm.compact

import io.github.leibnizhu.tinylsm.iterator.{MergeIterator, SsTableIterator, TwoMergeIterator}
import io.github.leibnizhu.tinylsm.{LsmStorageInner, LsmStorageState, MemTableKey, SsTable}

import scala.collection.mutable

case class FullCompactionTask(l0SsTableIds: List[Int], l1SsTableIds: List[Int]) extends CompactionTask {
  override def doCompact(storage: LsmStorageInner): List[SsTable] = {
    val ssTableMap = storage.state.ssTables
    val l0Iters = l0SsTableIds.map(ssTableMap(_)).map(SsTableIterator.createAndSeekToFirst)
    val l1Iters = l1SsTableIds.map(ssTableMap(_)).map(SsTableIterator.createAndSeekToFirst)
    val l1SsTables = l1SsTableIds.map(ssTableMap(_))
    val iter = TwoMergeIterator(
      MergeIterator(l0Iters), MergeIterator(l1Iters)
      //SstConcatIterator.createAndSeekToFirst(l1SsTables)
    )
    storage.compactGenerateSstFromIter(iter, compactToBottomLevel())
  }

  override def applyCompactionResult(state: LsmStorageState, output: List[Int]): List[Int] = {
    // 修改 levels 1
    state.levels = state.levels.updated(0, (1, output))

    // 更新 L0
    val l0SsTablesSet = mutable.HashSet(l0SsTableIds: _*)
    state.l0SsTables = state.l0SsTables.filter(s => !l0SsTablesSet.remove(s))
    assert(l0SsTablesSet.isEmpty)
    l0SsTableIds ++ l1SsTableIds
  }

  override def compactToBottomLevel(): Boolean = true
}
