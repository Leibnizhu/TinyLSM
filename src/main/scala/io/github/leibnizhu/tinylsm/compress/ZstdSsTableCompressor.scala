package io.github.leibnizhu.tinylsm.compress

import com.github.luben.zstd.*
import io.github.leibnizhu.tinylsm.MemTableValue
import io.github.leibnizhu.tinylsm.compress.CompressState.{Compress, Decompress, Train}
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.AtomicInteger

class ZstdSsTableCompressor(sampleSize: Int = 1024 * 1024, dictSize: Int = 16 * 1024) extends SsTableCompressor {
  private val log = LoggerFactory.getLogger(this.getClass)

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

  override def addDictSample(sample: MemTableValue): Unit = if (isState(Train)) {
    val cnt = sampleCnt.incrementAndGet()
    // 前20条全部采集，保证采样率
    if (cnt <= 20 || cnt % 100 == 0) {
      dictTrainer.addSample(sample)
    }
  }

  override def generateDict(): Array[Byte] = try {
    val rawDict = dictTrainer.trainSamples
    loadDict(rawDict)
    rawDict
  } catch
    case e: ZstdException =>
      // 可能采样数量不够，无法训练字典
      log.error("Train zstd dict failed: {}", e.getMessage)
      Array()

  override def compress(origin: Array[Byte]): Array[Byte] = state match
    case Decompress => throw new IllegalStateException("ZstdSsTableCompressor is not in compress state")
    case Train => origin
    case Compress => compressCtx.compress(origin)

  override def decompress(compressed: Array[Byte], originLength: Int): Array[Byte] = state match
    // 如果是同个Compressor在压缩或训练的模式下调用 decompress，那么应该是在读取未压缩数据进行压缩，直接返回原始数据即可
    case Train | Compress => compressed
    case Decompress => decompressCtx.decompress(compressed, originLength)

  override def close(): Unit = {
    compressCtx.close()
    decompressCtx.close()
  }

  override val DICT_TYPE: Byte = ZstdSsTableCompressor.DICT_TYPE
}

object ZstdSsTableCompressor {
  val DICT_TYPE: Byte = 1
}