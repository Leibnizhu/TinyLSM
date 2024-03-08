package io.github.leibnizhu.tinylsm.compact

import io.github.leibnizhu.tinylsm.iterator.{MergeIterator, SstConcatIterator}
import io.github.leibnizhu.tinylsm.{Level, LsmStorageInner, LsmStorageState, SsTable}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer
import scala.util.boundary

case class TieredCompactionTask(tiers: List[Level],
                                bottomTierIncluded: Boolean,
                               ) extends CompactionTask {
  override def doCompact(storage: LsmStorageInner): List[SsTable] = {
    val snapshot = storage.state.copy()
    val iters = tiers.map((_, sstIds) =>
      SstConcatIterator.createAndSeekToFirst(sstIds.map(snapshot.ssTables(_))))
    val mergeIter = MergeIterator(iters)
    storage.compactGenerateSstFromIter(mergeIter, bottomTierIncluded)
  }

  override def applyCompactionResult(state: LsmStorageState, output: List[Int]): List[Int] = {
    val snapshot = state.copy()
    assert(snapshot.l0SsTables.isEmpty, "should not add l0 ssts in tiered compaction")
    val compactedTierMap = tiers.toMap
    // 注意此时snapshot的levels可能已经有了新的tier，需要找到当前task的tiers对应的位置，删除，替换为新tier
    val newLevel = new ListBuffer[Level]()
    val sstToDelete = new ListBuffer[Int]()
    for ((tierId, ssts) <- snapshot.levels) {
      if (compactedTierMap.contains(tierId)) {
        // 当前遍历的tier在compact任务中，要删除
        if (sstToDelete.isEmpty) {
          // sstToDelete 为空时，即遍历到compact的第一个Tier，应放入新Tier
          newLevel += ((output.head, output))
        }
        sstToDelete ++= compactedTierMap(tierId)
      } else {
        // 否则保留在新levels中
        newLevel += ((tierId, ssts))
      }
    }
    state.levels = newLevel.toList
    sstToDelete.toList
  }

  override def compactToBottomLevel(): Boolean = bottomTierIncluded
}

object TieredCompactionTask {
  private val log = LoggerFactory.getLogger(this.getClass)

  /**
   *
   * @param options  Compaction配置
   * @param snapshot Lsm内部状态 snapshot
   * @return 压缩任务。如果无需Compaction则返回None
   */
  def generate(options: CompactionOptions.TieredCompactionOptions, snapshot: LsmStorageState): Option[TieredCompactionTask] = {
    assert(snapshot.l0SsTables.isEmpty, "should not add l0 ssts in tiered compaction")
    val levels = snapshot.levels
    if (levels.length < options.numTiers) {
      // tier 层数不够多
      return None
    }
    // 1. 考虑空间放大率 space amplification ratio
    // 应该是 engineSize / lastLevelSize 实际可以用 除最底以外所有level的总size / 最底level的size
    val lowestSize = levels.last._2.length
    val allUpperSize = levels.map(_._2.length).sum - lowestSize
    val spaceAmpRatio = 100 * allUpperSize.toDouble / lowestSize
    if (spaceAmpRatio >= options.maxSizeAmplificationPercent) {
      log.info("Compaction triggered by space amplification ratio: {}", "%.3f".format(spaceAmpRatio))
      return Some(TieredCompactionTask(List(levels: _*), true))
    }

    // 2. 考虑 size 比例
    // 记录当前的所有上层的总大小
    val sizeRatioThreshold = (100.0 + options.sizeRatio.toDouble) / 100
    var upperSizeSum = 0
    val sizeRationLevel = {
      boundary:
        for (id <- 0 until levels.length - 1) {
          upperSizeSum += levels(id)._2.length
          val curSizeRatio = upperSizeSum.toDouble / levels(id + 1)._2.length
          // 两个条件，既要sizeRatio超了阈值，同时要compact的层数超过 minMergeWidth。
          // 因为id 是从0开始的，当前一共 id+1层，加上下一层，所以是 id +2
          if (curSizeRatio >= sizeRatioThreshold && id + 2 >= options.minMergeWidth) {
            log.info("Compaction triggered by size ratio: {}", "%.3f".format(curSizeRatio * 100))
            boundary.break(Some(id + 2))
          }
        }
        None
    }
    if (sizeRationLevel.isDefined) {
      return Some(TieredCompactionTask(levels.take(sizeRationLevel.get), sizeRationLevel.get >= levels.length))
    }

    // 最后确保 tier 数量少于 numTiers
    if (snapshot.levels.length > options.numTiers) {
      val numTiersToTake = snapshot.levels.length - options.numTiers + 2
      // FIXME rust 代码里是 levels.length >= numTiersToTake
      Some(TieredCompactionTask(levels.take(numTiersToTake), numTiersToTake >= levels.length))
    } else {
      None
    }
  }
}