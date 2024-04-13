package io.github.leibnizhu.tinylsm.compress

import io.github.leibnizhu.tinylsm.MemTableValue
import io.github.leibnizhu.tinylsm.compress.CompressState.{Compress, Decompress, Train}

import java.util.zip.{Deflater, Inflater}

/**
 * Java的Zlib实现（即java.util.zip.Deflater）并不直接支持动态字典。
 * 但可以使用JNI（Java Native Interface）来调用底层的Zlib库，并在其中设置动态字典
 */
class ZlibSsTableCompressor(level: Int = Deflater.DEFAULT_COMPRESSION) extends SsTableCompressor {

  override val DICT_TYPE: Byte = ZlibSsTableCompressor.DICT_TYPE

  override def needTrainDict(): Boolean = false

  override def addDictSample(sample: MemTableValue): Unit = {}

  override def generateDict(): Array[Byte] = Array()

  override def compress(origin: Array[Byte]): Array[Byte] = state match
    case Decompress => throw new IllegalStateException("ZlibSsTableCompressor is not in compress state")
    case Train | Compress => doCompress(origin)

  private def doCompress(origin: Array[Byte]) = {
    val deflater = new Deflater(level)
    deflater.setInput(origin)
    deflater.finish()
    val compressedData = new Array[Byte](origin.length)
    val compressedSize = deflater.deflate(compressedData)
    val compressed = new Array[Byte](compressedSize)
    System.arraycopy(compressedData, 0, compressed, 0, compressedSize)
    deflater.end()
    compressed
  }

  override def decompress(compressed: Array[Byte], originLength: Int): Array[Byte] = state match
    // 如果是同个Compressor在压缩或训练的模式下调用 decompress，那么应该是在读取未压缩数据进行压缩，直接返回原始数据即可
    case Train | Compress => compressed
    case Decompress => doDecompress(compressed, originLength)

  private def doDecompress(compressed: Array[Byte], originLength: Int) = {
    val inflater = new Inflater()
    inflater.setInput(compressed)
    val decompressed = new Array[Byte](originLength)
    val decompressedSize = inflater.inflate(decompressed)
    inflater.end()
    decompressed
  }

  override def close(): Unit = {}

  override def toString: String = s"ZLib(level $level)"
}

object ZlibSsTableCompressor {
  val DICT_TYPE: Byte = 2
}