package io.github.leibnizhu.tinylsm.compact

import io.github.leibnizhu.tinylsm.iterator.{MergeIterator, SsTableIterator, SstConcatIterator, TwoMergeIterator}
import io.github.leibnizhu.tinylsm.{LsmStorageInner, LsmStorageState, SsTable}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

case class SimpleCompactionTask(
                                 // None 则对应 L0 compaction
                                 upperLevel: Option[Int],
                                 upperLevelSstIds: List[Int],
                                 lowerLevel: Int,
                                 lowerLevelSstIds: List[Int],
                                 isLowerLevelBottomLevel: Boolean,
                               ) extends CompactionTask {
  override def doCompact(storage: LsmStorageInner): List[SsTable] = this.upperLevel match
    case None => SimpleCompactionTask.doCompactL0(storage, upperLevelSstIds, lowerLevelSstIds, isLowerLevelBottomLevel)
    case Some(level) => SimpleCompactionTask.doCompactLevel(storage, level, upperLevelSstIds, lowerLevelSstIds, isLowerLevelBottomLevel)

  override def applyCompactionResult(state: LsmStorageState, output: List[Int]): List[Int] = {
    val snapshot = state.copy()
    val sstToRemove = ListBuffer[Int]()
    var tempLevels = snapshot.levels
    var newL0 = Option[List[Int]](null)
    this.upperLevel match
      case None =>
        // 应用 L0 compaction 结果
        // L0的sst要移除
        sstToRemove ++= upperLevelSstIds
        // compact时 L0 可能被更新，所以这里从当前 l0SsTables 移除compact任务处理的sst
        newL0 = Some(snapshot.l0SsTables.filter(!upperLevelSstIds.contains(_)))
      case Some(level) =>
        // 应用 L1 以上level的 compaction 结果
        val upLevel = upperLevel.get
        assert(upperLevelSstIds == snapshot.levels(upLevel - 1)._2,
          "upper level's compact SST ID mismatch")
        // 上一级的sst要移除
        sstToRemove ++= upperLevelSstIds
        // 上一级的level变空
        tempLevels = tempLevels.updated(upLevel - 1, (upLevel, List()))
    assert(lowerLevelSstIds == snapshot.levels(lowerLevel - 1)._2,
      "lower level's compact SST ID mismatch")
    sstToRemove ++= snapshot.levels(lowerLevel - 1)._2
    tempLevels = tempLevels.updated(lowerLevel - 1, (lowerLevel, output))
    //    println(s"old level: ${state.levels}, new level: ${tempLevels}")
    state.write(st => {
      st.levels = tempLevels
      newL0.foreach(st.l0SsTables = _)
    })
    sstToRemove.toList
  }

  override def compactToBottomLevel(): Boolean = isLowerLevelBottomLevel
}


object SimpleCompactionTask {
  private val log = LoggerFactory.getLogger(classOf[SimpleCompactionTask])

  /**
   * 注意SimpleCompactionTask中SST顺序
   *
   * @param options  Compaction配置
   * @param snapshot Lsm内部状态 snapshot
   * @return 压缩任务。如果无需Compaction则返回None
   */
  def generate(options: CompactionOptions.SimpleCompactionOptions, snapshot: LsmStorageState): Option[SimpleCompactionTask] = {
    val levelSizes = snapshot.l0SsTables.size :: snapshot.levels.map(_._2.size)
    // i = level几
    var targetLevel: Option[Int] = None
    for (curLevel <- 0 until options.maxLevels) {
      // 如果是level0，要看L0 sst 数量是否满足 L0=>L1 压缩阈值
      if (targetLevel.isEmpty && (curLevel != 0 || snapshot.l0SsTables.length >= options.level0FileNumCompactionTrigger)) {
        // 如果满足条件，当前是要 Level(i) + Level(i+1) 合并到 Level(i+1)
        val lowerLevel = curLevel + 1
        val sizeRatio = levelSizes(lowerLevel).toDouble / levelSizes(curLevel).toDouble
        if (sizeRatio < options.sizeRatioPercent.toDouble / 100.0) {
          log.info("Compaction triggered at level {} and {} with size ratio {}", curLevel, lowerLevel, sizeRatio)
          targetLevel = Some(curLevel)
        }
      }
    }
    if (targetLevel.isDefined) {
      val curLevel = targetLevel.get
      val lowerLevel = curLevel + 1
      Some(SimpleCompactionTask(
        upperLevel = if (curLevel == 0) None else Some(curLevel),
        upperLevelSstIds = if (curLevel == 0) snapshot.l0SsTables else snapshot.levels(curLevel - 1)._2,
        lowerLevel = lowerLevel,
        lowerLevelSstIds = snapshot.levels(lowerLevel - 1)._2,
        isLowerLevelBottomLevel = lowerLevel == options.maxLevels
      ))
    } else {
      //      snapshot.dumpState()
      None
    }
  }

  def doCompactL0(storage: LsmStorageInner, upperLevelSstIds: List[Int], lowerLevelSstIds: List[Int], isLowerLevelBottomLevel: Boolean): List[SsTable] = {
    val snapshot = storage.state.copy()
    // upper 是L0, 没有compact过，所以不能用 SstConcatIterator，得用 MergeIterator
    val upperIter = MergeIterator(upperLevelSstIds.map(snapshot.ssTables(_))
      .map(SsTableIterator.createAndSeekToFirst))
    val lowerIter = SstConcatIterator.createAndSeekToFirst(lowerLevelSstIds.map(snapshot.ssTables(_)))
    storage.compactGenerateSstFromIter(TwoMergeIterator(upperIter, lowerIter), isLowerLevelBottomLevel)
  }

  def doCompactLevel(storage: LsmStorageInner, level: Int, upperLevelSstIds: List[Int], lowerLevelSstIds: List[Int], isLowerLevelBottomLevel: Boolean): List[SsTable] = {
    val snapshot = storage.state.copy()
    // upper lower 都是L1或以上，可以都用 SstConcatIterator 提高效率
    val upperIter = SstConcatIterator.createAndSeekToFirst(upperLevelSstIds.map(snapshot.ssTables(_)))
    val lowerIter = SstConcatIterator.createAndSeekToFirst(lowerLevelSstIds.map(snapshot.ssTables(_)))
    storage.compactGenerateSstFromIter(TwoMergeIterator(upperIter, lowerIter), isLowerLevelBottomLevel)
  }
}