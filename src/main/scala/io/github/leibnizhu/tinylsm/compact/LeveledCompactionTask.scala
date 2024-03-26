package io.github.leibnizhu.tinylsm.compact

import io.github.leibnizhu.tinylsm.{LsmStorageInner, LsmStorageState, SsTable}
import org.slf4j.LoggerFactory

import scala.collection.mutable


case class LeveledCompactionTask(
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
    val upperLevelSstIdsSet = new mutable.HashSet[Int]() ++ upperLevelSstIds
    if (upperLevel.isDefined) {
      val newUpperSsts = snapshot.levels(upperLevel.get - 1)._2.filter(s => if (upperLevelSstIdsSet.contains(s)) {
        upperLevelSstIdsSet -= s
        false
      } else {
        true
      })
      assert(upperLevelSstIdsSet.isEmpty)
      state.write(st => st.levels = st.levels.updated(upperLevel.get - 1, (upperLevel.get, newUpperSsts)))
    } else {
      // L0 合并
      val newL0Ssts = snapshot.l0SsTables.filter(s => if (upperLevelSstIdsSet.contains(s)) {
        upperLevelSstIdsSet -= s
        false
      } else {
        true
      })
      assert(upperLevelSstIdsSet.isEmpty)
      state.write(st => st.l0SsTables = newL0Ssts)
    }
    val sstToRemove = upperLevelSstIds ++ lowerLevelSstIds

    val lowerLevelSstIdsSet = new mutable.HashSet[Int]() ++ lowerLevelSstIds
    val newLowerLevelSsts = snapshot.levels(lowerLevel - 1)._2.filter(s => if (lowerLevelSstIdsSet.contains(s)) {
      lowerLevelSstIdsSet -= s
      false
    } else {
      true
    })
    assert(lowerLevelSstIdsSet.isEmpty)
    val finalLowerLevel = (newLowerLevelSsts ++ output).sortBy(sstId => snapshot.ssTables(sstId).firstKey)
    state.write(st => st.levels = st.levels.updated(lowerLevel - 1, (lowerLevel, finalLowerLevel)))

    sstToRemove
  }

  override def compactToBottomLevel(): Boolean = true
}

object LeveledCompactionTask {
  private val log = LoggerFactory.getLogger(this.getClass)

  /**
   *
   * @param options  Compaction配置
   * @param snapshot Lsm内部状态 snapshot
   * @return 压缩任务。如果无需Compaction则返回None
   */
  def generate(options: CompactionOptions.LeveledCompactionOptions, snapshot: LsmStorageState): Option[LeveledCompactionTask] = {
    // 1. 计算level的 targetSize
    // 每个level的实际大小
    val realLevelSize = (0 until options.maxLevels).map(level => snapshot.levels(level)._2
      .map(snapshot.ssTables(_).tableSize()).sum)
    val baseLevelSizeBytes = options.baseLevelSizeMb * 1024 * 1024
    val lastLevelTargetSize = realLevelSize(options.maxLevels - 1).max(baseLevelSizeBytes)
    // 从底部开始遍历,查找 baseLevel，(1-N，不是下标，是level层数)
    var baseLevel = options.maxLevels
    val targetLevelSize = (options.maxLevels - 2 to 0 by -1).foldLeft(List(lastLevelTargetSize))((levelSizes, i) => {
      // levelSizes 是已经计算的 targetSize 数组，i+1 是当前的level数 
      val curLevelSize = levelSizes.head / options.levelSizeMultiplier
      //      println(s"${levelSizes} ==> $curLevelSize")
      if (levelSizes.head > baseLevelSizeBytes) {
        baseLevel = i + 1
        curLevelSize :: levelSizes
      } else {
        // 如果下一层level不到 baseLevelSizeMb ，那么再上一层就不用处理了，targetSize == 0
        0 :: levelSizes
      }
    })

    // L0 合并的优先级最高
    if (snapshot.l0SsTables.length >= options.level0FileNumCompactionTrigger) {
      log.info("flush L0 SST to base level {}", baseLevel)
      val lowerSstIds = findOverlappingSsts(snapshot, snapshot.l0SsTables, baseLevel)
      return Some(LeveledCompactionTask(
        upperLevel = None, upperLevelSstIds = List(snapshot.l0SsTables: _*),
        lowerLevel = baseLevel, lowerLevelSstIds = lowerSstIds,
        isLowerLevelBottomLevel = baseLevel == options.maxLevels))
    }

    // 根据 curSize / targetSize 计算优先级, 过滤 >1 的最大的
    val priorities = (0 until options.maxLevels)
      .map(l => (if (targetLevelSize(l) == 0) 0 else realLevelSize(l).toDouble / targetLevelSize(l), l + 1))
      .filter(_._1 > 1.0).sortBy(_._1).reverse
    if (priorities.nonEmpty) {
      val (priority, level) = priorities.head
      log.info("target level sizes: {}, real level sizes: {}, base level: {}",
        targetLevelSize.map(size => "%.3fMB".format(size.toDouble / 1024.0 / 1024.0)),
        realLevelSize.map(size => "%.3fMB".format(size.toDouble / 1024.0 / 1024.0)), baseLevel)
      val selectedSst = snapshot.levels(level - 1)._2.min
      log.info("compaction triggered by priority: {} out of {}, select {} for compaction", level, "%.4f".format(priority), selectedSst)
      val lowerSstIds = findOverlappingSsts(snapshot, List(selectedSst), level + 1)
      Some(LeveledCompactionTask(
        upperLevel = Some(level), upperLevelSstIds = List(selectedSst),
        lowerLevel = level + 1, lowerLevelSstIds = lowerSstIds,
        isLowerLevelBottomLevel = level + 1 == options.maxLevels))
    } else {
      None
    }
  }

  /**
   *
   * @param snapshot Lsm 状态
   * @param sstIds   需要查询的sst
   * @param inLevel  需要查询的level数
   * @return 在指定level里面找能覆盖指定sst对应key范围的sst
   */
  private def findOverlappingSsts(snapshot: LsmStorageState, sstIds: List[Int], inLevel: Int): List[Int] = {
    val beginKey = sstIds.map(snapshot.ssTables(_).firstKey).min
    val endKey = sstIds.map(snapshot.ssTables(_).lastKey).max
    snapshot.levels(inLevel - 1)._2
      .map(snapshot.ssTables(_))
      // 过滤key范围有重叠的
      .filter(sst => sst.firstKey.compareTo(endKey) <= 0
        && sst.lastKey.compareTo(beginKey) >= 0)
      .map(_.sstId())
  }

}