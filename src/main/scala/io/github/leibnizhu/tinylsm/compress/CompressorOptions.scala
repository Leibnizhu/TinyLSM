package io.github.leibnizhu.tinylsm.compress

import io.github.leibnizhu.tinylsm.utils.Config

enum CompressorOptions {

  /**
   * 不做压缩
   */
  case None extends CompressorOptions

  case Zstd(
             trainDict: Boolean = true,
             sampleSize: Int = 1024 * 1024,
             dictSize: Int = 16 * 1024,
             level: Int = 3
           ) extends CompressorOptions

  case Zlib(level: Int = -1) extends CompressorOptions

  case Lz4(level: Int = -1) extends CompressorOptions
}

object CompressorOptions {
  def fromConfig(): CompressorOptions = {
    Config.CompressorType.get().toLowerCase match {
      case "zstd" => Zstd(
        trainDict = Config.CompressorZstdTrainDict.getBoolean,
        sampleSize = Config.CompressorZstdSampleSize.getInt,
        dictSize = Config.CompressorZstdDictSize.getInt,
        level = Config.CompressorZstdLevel.getInt,
      )
      case "zlib" => Zlib(Config.CompressorZlibLevel.getInt)
      case "lz4" => Lz4(Config.CompressorLz4Level.getInt)
      case "none" | "no" => None
      case s: String => throw new IllegalArgumentException("Unsupported compaction strategy: " + s)
    }
  }
}