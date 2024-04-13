package io.github.leibnizhu.tinylsm.compress

import io.github.leibnizhu.tinylsm.MemTableValue
import io.github.leibnizhu.tinylsm.compress.CompressState.{Compress, Decompress, Train}
import net.jpountz.lz4.LZ4Factory

class Lz4SsTableCompressor(level: Int = -1) extends SsTableCompressor {
  private val (compressor, decompressor) = {
    val factory = LZ4Factory.fastestInstance()
    (if (level == -1) factory.fastCompressor() else factory.highCompressor(level), factory.fastDecompressor())
  }

  override def needTrainDict(): Boolean = false

  override val DICT_TYPE: Byte = Lz4SsTableCompressor.DICT_TYPE

  override def addDictSample(sample: MemTableValue): Unit = {}

  override def generateDict(): Array[Byte] = Array()

  override def compress(origin: Array[Byte]): Array[Byte] = state match
    case Decompress => throw new IllegalStateException("Lz4SsTableCompressor is not in compress state")
    case Train | Compress => compressor.compress(origin)

  override def decompress(compressed: Array[Byte], originLength: Int): Array[Byte] = state match
    // 如果是同个Compressor在压缩或训练的模式下调用 decompress，那么应该是在读取未压缩数据进行压缩，直接返回原始数据即可
    case Train | Compress => compressed
    case Decompress => decompressor.decompress(compressed, originLength)

  override def close(): Unit = {}

  override def toString: String = s"LZ4(${if (level == -1) "fast" else ("level " + level)})"

}

object Lz4SsTableCompressor {
  val DICT_TYPE: Byte = 3
}