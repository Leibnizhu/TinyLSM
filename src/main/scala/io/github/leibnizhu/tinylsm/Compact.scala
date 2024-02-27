package io.github.leibnizhu.tinylsm

class Compact {

}

abstract sealed class CompactionTask {
  def doCompact(storage: LsmStorageInner): List[SsTable]

  def compactToBottomLevel(): Boolean
}

case class LeveledCompactionTask() extends CompactionTask {
  override def doCompact(storage: LsmStorageInner): List[SsTable] = {
    // TODO
    null
  }

  override def compactToBottomLevel(): Boolean = true
}

case class TieredCompactionTask() extends CompactionTask {
  override def doCompact(storage: LsmStorageInner): List[SsTable] = {
    // TODO
    null
  }

  override def compactToBottomLevel(): Boolean = true
}

case class SimpleCompactionTask() extends CompactionTask {
  override def doCompact(storage: LsmStorageInner): List[SsTable] = {
    // TODO
    null
  }

  override def compactToBottomLevel(): Boolean = true
}

case class ForceFullCompactionTask(l0SsTableIds: List[Int], l1SsTableIds: List[Int]) extends CompactionTask {
  override def doCompact(storage: LsmStorageInner): List[SsTable] = {
    val ssTableMap = storage.state.ssTables
    val l0Iters = l0SsTableIds.map(ssTableMap(_)).map(SsTableIterator.createAndSeekToFirst)
    val l1Iters = l1SsTableIds.map(ssTableMap(_)).map(SsTableIterator.createAndSeekToFirst)
    val l1SsTables = l1SsTableIds.map(ssTableMap(_))
    val iter = TwoMergeIterator[MergeIterator[SsTableIterator], MergeIterator[SsTableIterator]](
      MergeIterator(l0Iters),  MergeIterator(l1Iters)
      //SstConcatIterator.createAndSeekToFirst(l1SsTables)
    )
    storage.compactGenerateSstFromIter(iter, compactToBottomLevel())
  }

  override def compactToBottomLevel(): Boolean = true
}

enum CompactionOptions {

  case NoCompaction extends CompactionOptions

  case LeveledCompactionOptions(
                                 levelSizeMultiplier: Int,
                                 level0FileNumCompactionTrigger: Int,
                                 maxLevels: Int,
                                 baseLevelSizeMb: Int,
                               ) extends CompactionOptions

  case TieredCompactionOptions(
                                numTiers: Int,
                                maxSizeAmplificationPercent: Int,
                                sizeRatio: Int,
                                minMergeWidth: Int,
                              ) extends CompactionOptions

  case SimpleCompactionOptions(
                                sizeRatioPercent: Int,
                                level0FileNumCompactionTrigger: Int,
                                maxLevels: Int,
                              ) extends CompactionOptions
}