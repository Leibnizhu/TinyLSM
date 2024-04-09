package io.github.leibnizhu.tinylsm.compress

import io.github.leibnizhu.tinylsm.MemTableValue
import io.github.leibnizhu.tinylsm.block.BlockMeta
import io.github.leibnizhu.tinylsm.utils.ByteArrayWriter
import org.slf4j.LoggerFactory

trait SsTableCompressor {

  /**
   * 每次put k-v都会调用，实现自己按需按比例采样
   *
   * @param sample 样本
   */
  def addDictSample(sample: MemTableValue): Unit

  /**
   * 生成动态字典
   *
   * @return 动态字典
   */
  def generateDict(): (Byte, Array[Byte])

  /**
   * 压缩SSTable
   *
   * @param blockData 未压缩的block数据
   * @param meta      BlockMeta们
   * @return (已压缩的block数据, 与压缩后block数据匹配的Array[BlockMeta])
   */
  def compressSsTable(blockData: ByteArrayWriter, meta: Array[BlockMeta]): (ByteArrayWriter, Array[BlockMeta])

  def decompress(compressed: Array[Byte], originLength: Int): Array[Byte]
}

object SsTableCompressor {
  private val log = LoggerFactory.getLogger(this.getClass)
  val NONE_COMPRESSOR: Byte = 0

  def recover(rawDict: Array[Byte]): Option[SsTableCompressor] = if (rawDict.isEmpty) None else {
    val dictType = rawDict.head
    dictType match
      case 1 =>
        val dict = rawDict.slice(1, rawDict.length)
        Some(ZstdSsTableCompressor().loadDict(dict))
      case 0 => None
      case _ =>
        log.error("Unsupported SsTableCompressor type: {}", dictType)
        None
  }
}
