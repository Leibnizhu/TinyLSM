package io.github.leibnizhu.tinylsm.compress

import com.github.luben.zstd.{ZstdDecompressCtx, ZstdDictTrainer}
import io.github.leibnizhu.tinylsm.MemTableValue
import io.github.leibnizhu.tinylsm.block.BlockMeta
import io.github.leibnizhu.tinylsm.utils.ByteArrayWriter

import java.util.concurrent.atomic.AtomicInteger

class ZstdSsTableCompressor extends SsTableCompressor {

  private val dictTrainer = new ZstdDictTrainer(1024 * 1024, 16 * 1024)
  private val sampleCnt = new AtomicInteger(0)
  private var dict: ZstdDecompressCtx = _

  def loadDict(oldDict: Array[Byte]): ZstdSsTableCompressor = {
    this.dict = new ZstdDecompressCtx()
    this.dict.loadDict(oldDict)
    this
  }

  override def addDictSample(sample: MemTableValue): Unit = {
    if (sampleCnt.incrementAndGet() % 100 == 0) {
      dictTrainer.addSample(sample)
    }
  }

  override def generateDict(): (Byte, Array[Byte]) = {
    val rawDict = dictTrainer.trainSamples
    loadDict(rawDict)
    (1.toByte, rawDict)
  }

  override def compressSsTable(blockData: ByteArrayWriter, meta: Array[BlockMeta]): (ByteArrayWriter, Array[BlockMeta]) = ???

  override def decompress(compressed: Array[Byte], originLength: Int): Array[Byte] = {
    if (dict == null) {
      throw new IllegalStateException("ZSTD dictionary is not init yet, plz call generateDict() or loadDict()")
    }
    dict.decompress(compressed, originLength)
  }
}
