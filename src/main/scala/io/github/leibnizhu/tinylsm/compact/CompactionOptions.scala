package io.github.leibnizhu.tinylsm.compact

import io.github.leibnizhu.tinylsm.utils.Config

enum CompactionOptions {

  /**
   * 不做任何compaction
   */
  case NoCompaction extends CompactionOptions

  /**
   * 全 Compaction，每次都是L0+L1全部 compact 成新的L1
   */
  case FullCompaction extends CompactionOptions

  /**
   * Tiered 和 Simple 在合并压缩时都无法删除旧文件，需要占用两倍空间
   * 另外如果中间的level为空的话，Simple策略会将一个sst从L0一路刷到最底部，
   * Leveled 根据 baseLevelSizeMb 和 levelSizeMultiplier、maxLevels 维护每一层的 targetSize
   * 压缩L0时，选择第一个 targetSize > 0 的level进行压缩
   * 优先级：
   * 1. L0 压缩有最高优先级，达到阈值后优先和其他level合并
   * 2. 通过 curSize / targetSize 计算每个level的压缩优先级，只压缩 > 1.0 的，并选择比例最大的level与其下一级进行合并
   * 决定压缩level后，从上层取最旧的sst（也可以用其他方法决定sst，如逻辑删除的数量）
   * 然后从下层找所有与之key范围重叠的sst（如果key分布均匀，下层level 应该大概是有 levelSizeMultiplier 个对应sst）
   *
   */
  case LeveledCompactionOptions(
                                 levelSizeMultiplier: Int,

                                 /**
                                  * L0 sst文件数量到达这个阈值触发压缩
                                  */
                                 level0FileNumCompactionTrigger: Int,
                                 maxLevels: Int,

                                 /**
                                  * 启用一个新level的阈值。最多只能有一个非空level的大小小于 baseLevelSizeMb
                                  * 在最底层超过 baseLevelSizeMb 之前，其他中间级别的目标大小都将为 0
                                  * 当最底层 >= baseLevelSizeMb，倒数第二层目标大小是 baseLevelSizeMb / levelSizeMultiplier
                                  * 比如说 maxLevel=3, baseLevelSizeMb=100MB, levelSizeMultiplier=10
                                  * 那么一开始 [0,0,0] => [0,0,100MB] 可以启用新level => [0,10MB,100MB]
                                  * 逐渐地 [0,100MB,1000GB] 启用新level => [10MB,100MB,1000MB]
                                  */
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

object CompactionOptions {
  def fromConfig(): CompactionOptions = {
    Config.CompactionStrategy.get().toLowerCase match {
      case "leveled" => LeveledCompactionOptions(
        levelSizeMultiplier = Config.CompactionLevelSizeMultiplier.getInt,
        level0FileNumCompactionTrigger = Config.CompactionLevel0FileNumTrigger.getInt,
        maxLevels = Config.CompactionMaxLevels.getInt,
        baseLevelSizeMb = Config.CompactionBaseLevelSizeMb.getInt
      )
      case "simple" => SimpleCompactionOptions(
        sizeRatioPercent = Config.CompactionSizeRatioPercent.getInt,
        level0FileNumCompactionTrigger = Config.CompactionLevel0FileNumTrigger.getInt,
        maxLevels = Config.CompactionMaxLevels.getInt
      )
      case "tiered" => TieredCompactionOptions(
        maxSizeAmplificationPercent = Config.CompactionMaxSizeAmpPercent.getInt,
        sizeRatio = Config.CompactionSizeRatioPercent.getInt,
        minMergeWidth = Config.CompactionMinMergeWidth.getInt,
        numTiers = Config.CompactionMaxLevels.getInt
      )
      case "full" => FullCompaction
      case "none" | "no" => NoCompaction
      case s: String => throw new IllegalArgumentException("Unsupported compaction strategy: " + s)
    }
  }
}
