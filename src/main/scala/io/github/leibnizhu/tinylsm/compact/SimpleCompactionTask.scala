package io.github.leibnizhu.tinylsm.compact

import io.github.leibnizhu.tinylsm.{LsmStorageInner, LsmStorageState, SsTable}
import org.slf4j.LoggerFactory

import scala.util.boundary

case class SimpleCompactionTask(
                                 // None 则对应 L0 compaction
                                 upperLevel: Option[Int],
                                 upperLevelSstIds: List[Int],
                                 lowerLevel: Int,
                                 lowerLevelSstIds: List[Int],
                                 isLowerLevelBottomLevel: Boolean,
                               ) extends CompactionTask {
  override def doCompact(storage: LsmStorageInner): List[SsTable] = upperLevel match
    case None => doCompactL0()
    case Some(level) => doCompactLevel(level)

  private def doCompactL0(): List[SsTable] = {
    null
  }

  private def doCompactLevel(level: Int): List[SsTable] = {
    null
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
  def apply(options: CompactionOptions.SimpleCompactionOptions, snapshot: LsmStorageState): Option[SimpleCompactionTask] = {
    val levelSizes = snapshot.l0SsTables.size :: snapshot.levels.map(_._2.size)
    // i = level几
    boundary:
      for (curLevel <- 0 until options.maxLevels) {
        // 如果是level0，要看L0 sst 数量是否满足 L0=>L1 压缩阈值
        if (curLevel != 0 || snapshot.l0SsTables.length >= options.level0FileNumCompactionTrigger) {
          // 如果满足条件，当前是要 Level(i) + Level(i+1) 合并到 Level(i+1)
          val lowerLevel = curLevel + 1
          val sizeRatio = levelSizes(lowerLevel).toDouble / levelSizes(curLevel).toDouble
          if (sizeRatio < options.sizeRatioPercent.toDouble / 100.0) {
            log.info("compaction triggered at level {} and {} with size ratio {}", curLevel, lowerLevel, sizeRatio);
            boundary.break(Some(SimpleCompactionTask(
              upperLevel = if (curLevel == 0) None else Some(curLevel),
              upperLevelSstIds = if (curLevel == 0) snapshot.l0SsTables else snapshot.levels(curLevel - 1)._2,
              lowerLevel = lowerLevel,
              lowerLevelSstIds = snapshot.levels(lowerLevel - 1)._2,
              isLowerLevelBottomLevel = lowerLevel == options.maxLevels
            )))
          }
        }
      }
    None
  }
}