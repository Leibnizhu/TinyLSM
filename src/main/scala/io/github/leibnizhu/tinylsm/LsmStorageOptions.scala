package io.github.leibnizhu.tinylsm

import io.github.leibnizhu.tinylsm.compact.CompactionOptions
import io.github.leibnizhu.tinylsm.compress.CompressorOptions
import io.github.leibnizhu.tinylsm.utils.Config

case class LsmStorageOptions
(
  // Block大小，单位是 bytes，应该小于或等于这个值
  blockSize: Int,
  // SST大小，单位是 bytes, 同时也是MemTable容量限制的近似值
  targetSstSize: Int,
  // Manifest大小，单位是 bytes
  targetManifestSize: Int,
  // MemTable在内存中的最多个数, 超过这么多MemTable后会 flush 到 L0
  numMemTableLimit: Int,
  // Compaction配置
  compactionOptions: CompactionOptions,
  // value压缩配置
  compressorOptions: CompressorOptions,
  // 是否启用WAL
  enableWal: Boolean,
  // Transaction 是否可序列化
  serializable: Boolean
)

object LsmStorageOptions {
  def defaultOption(): LsmStorageOptions = LsmStorageOptions(
    4096,
    2 << 20,
    1 << 20,
    50,
    CompactionOptions.NoCompaction,
    CompressorOptions.None,
    false,
    false)

  def fromConfig(): LsmStorageOptions = LsmStorageOptions(
    Config.BlockSize.getInt,
    Config.TargetSstSize.getInt,
    Config.TargetManifestSize.getInt,
    Config.MemTableLimitNum.getInt,
    CompactionOptions.fromConfig(),
    CompressorOptions.fromConfig(),
    Config.EnableWal.getBoolean,
    Config.Serializable.getBoolean
  )
}