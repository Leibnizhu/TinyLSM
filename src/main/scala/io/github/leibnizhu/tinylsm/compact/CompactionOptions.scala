package io.github.leibnizhu.tinylsm.compact

enum CompactionOptions {

  /**
   * 不做任何compaction
   */
  case NoCompaction extends CompactionOptions

  /**
   * 全 Compaction，每次都是L0+L1全部 compact 成新的L1
   */
  case FullCompaction extends CompactionOptions

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

  /**
   * 类似LSM论文的原始策略
   * 当L1及以上的level太大时，将这个level和下一个level合并
   */
  case SimpleCompactionOptions(
                                /**
                                 * 下层(level更大)文件数/上层文件数, 实际应该计算文件大小，这里简化为文件数量
                                 * 当实际比率太高（上层文件太多） < 这个阈值时，应该触发压缩
                                 */
                                sizeRatioPercent: Int,

                                /**
                                 * 当L0中的SST数量 >= 这个阈值时，触发L0和L1的压缩。
                                 */
                                level0FileNumCompactionTrigger: Int,

                                /**
                                 * LSM树的层数（不包括L0）
                                 */
                                maxLevels: Int,
                              ) extends CompactionOptions
}
