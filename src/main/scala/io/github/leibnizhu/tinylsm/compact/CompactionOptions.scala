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

  /**
   * 不使用L0，直接将新的SST写入一个 Tier，levels 包含所有 Tier，最小索引的是最新写入的
   * 每次 flush L0 SST都写到levels的最前面，Tier ID是对应第一个SST的ID
   */
  case TieredCompactionOptions(

                                /**
                                 * universal compaction 的第一个触发因素是空间放大率，应该是 engineSize / lastLevelSize
                                 * 实际可以用 除最底以外所有level的总size / 最底level的size，取值范围 [0, +inf)
                                 * 当这个比值 >= maxSizeAmplificationPercent 时触发 compaction
                                 */
                                maxSizeAmplificationPercent: Int,

                                /**
                                 * universal compaction第二个触发因素是层间大小比例
                                 * 对任意level，如果存在存在一个 Level n ，使
                                 * 更上面所有level的总size / 当前level的size >= (1 + size_ratio) * 100% ，
                                 * 且需要compact的层数超过 minMergeWidth 时，则对前面n个level一起compaction
                                 */
                                sizeRatio: Int,
                                minMergeWidth: Int,
                                /**
                                 * universal compaction第三个触发因素是 Tier数
                                 * Tier数 大于 numTiers 时触发 compaction
                                 * compaction 后 levels 层数不能超过这个值
                                 */
                                numTiers: Int,
                              ) extends CompactionOptions

  /**
   * 类似LSM论文的原始策略
   * L0 是根据sst数量超过 level0FileNumCompactionTrigger 时触发合并
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
