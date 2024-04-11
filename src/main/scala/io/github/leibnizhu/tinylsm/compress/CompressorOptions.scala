package io.github.leibnizhu.tinylsm.compress

import io.github.leibnizhu.tinylsm.utils.Config

enum CompressorOptions {

  /**
   * 不做压缩
   */
  case None extends CompressorOptions

  case Zstd(
             sampleSize: Int = 1024 * 1024,
             dictSize: Int = 16 * 1024
           ) extends CompressorOptions
}

object CompressorOptions {
  def fromConfig(): CompressorOptions = {
    Config.CompressorType.get().toLowerCase match {
      case "zstd" => Zstd(
        sampleSize = Config.CompressorZstdSampleSize.getInt,
        dictSize = Config.CompressorZstdDictSize.getInt
      )
      case "none" | "no" => None
      case s: String => throw new IllegalArgumentException("Unsupported compaction strategy: " + s)
    }
  }
}