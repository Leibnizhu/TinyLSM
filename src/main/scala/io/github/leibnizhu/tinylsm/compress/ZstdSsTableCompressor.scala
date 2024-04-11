package io.github.leibnizhu.tinylsm.compress

import com.github.luben.zstd.{Zstd, ZstdCompressCtx, ZstdDecompressCtx, ZstdDictTrainer}
import io.github.leibnizhu.tinylsm.MemTableValue

import java.util.concurrent.atomic.AtomicInteger

class ZstdSsTableCompressor(sampleSize: Int = 1024 * 1024, dictSize: Int = 16 * 1024) extends SsTableCompressor {

  private val dictTrainer = new ZstdDictTrainer(sampleSize, dictSize)
  private val sampleCnt = new AtomicInteger(0)
  private val decompressCtx: ZstdDecompressCtx = new ZstdDecompressCtx().setMagicless(false)
  private val compressCtx: ZstdCompressCtx = new ZstdCompressCtx().setMagicless(false)
    .setDictID(false).setLevel(Zstd.defaultCompressionLevel)

  def loadDict(dict: Array[Byte]): ZstdSsTableCompressor = {
    this.decompressCtx.loadDict(dict)
    this.compressCtx.loadDict(dict)
    this
  }

  override def addDictSample(sample: MemTableValue): Unit = {
    val cnt = sampleCnt.incrementAndGet()
    // 前20条全部采集，保证采样率
    if (cnt <= 20 || cnt % 100 == 0) {
      dictTrainer.addSample(sample)
    }
  }

  override def generateDict(): Array[Byte] = {
    val rawDict = dictTrainer.trainSamples
    loadDict(rawDict)
    rawDict
  }

  override def compress(origin: Array[Byte]): Array[Byte] = {
    if (compressCtx == null) {
      throw new IllegalStateException("ZSTD dictionary is not init yet, plz call generateDict() or loadDict()")
    }
    compressCtx.compress(origin)
  }

  override def decompress(compressed: Array[Byte], originLength: Int): Array[Byte] = {
    if (decompressCtx == null) {
      throw new IllegalStateException("ZSTD dictionary is not init yet, plz call generateDict() or loadDict()")
    }
    decompressCtx.decompress(compressed, originLength)
  }

  override def close(): Unit = {
    compressCtx.close()
    decompressCtx.close()
  }

  override val DICT_TYPE: Byte = ZstdSsTableCompressor.DICT_TYPE
}

object ZstdSsTableCompressor {
  val DICT_TYPE: Byte = 1
}