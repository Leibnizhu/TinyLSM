package io.github.leibnizhu.tinylsm.compress

import com.github.blemale.scaffeine.Scaffeine
import io.github.leibnizhu.tinylsm.block.{Block, BlockMeta}
import io.github.leibnizhu.tinylsm.iterator.SsTableIterator
import io.github.leibnizhu.tinylsm.utils.ByteArrayWriter
import io.github.leibnizhu.tinylsm.{MemTableValue, SsTable, SsTableBuilder}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

trait SsTableCompressor extends AutoCloseable {
  private val log = LoggerFactory.getLogger(this.getClass)

  val DICT_TYPE: Byte

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
  def generateDict(): Array[Byte]

  /**
   * 压缩SSTable
   *
   * @param blocks 未压缩的block们
   * @param meta   BlockMeta们
   * @return (已压缩的block数据, 与压缩后block数据匹配的Array[BlockMeta])
   */
  def compressSsTable(blockSize: Int, blocks: ArrayBuffer[Block], meta: Array[BlockMeta]): (ByteArrayWriter, Array[BlockMeta]) = {
    generateDict()
    val sstId = -1
    val blockCache = Scaffeine().maximumSize(meta.length).build[(Int, Int), Block]()
    blocks.zipWithIndex.foreach((block, idx) => blockCache.put((sstId, idx), block))
    val sst = SsTable(
      file = null,
      id = sstId,
      blockMeta = meta,
      blockMetaOffset = 0,
      blockCache = Some(blockCache),
      firstKey = meta.head.firstKey.copy(),
      lastKey = meta.last.lastKey.copy(),
      bloom = None,
      maxTimestamp = 0,
    )
    val sstIter = SsTableIterator.createAndSeekToFirst(sst)
    val newSstBuilder = SsTableBuilder(blockSize, this).compressMode()
    while (sstIter.isValid) {
      newSstBuilder.add(sstIter.key(), sstIter.value())
      sstIter.next()
    }
    newSstBuilder.finishBlock()
    log.info("Completed SST compression. Before compression: {} Blocks, about {} KB; After compression: {} Blocks, {} KB",
      blocks.length, "%.2f".format(blockSize * blocks.length / 1024.0),
      newSstBuilder.meta.length, "%.2f".format(newSstBuilder.data.length / 1024.0))
    (newSstBuilder.data, newSstBuilder.meta.toArray)
  }

  /**
   * 压缩单个值
   *
   * @param origin 原始数据
   * @return 压缩后数据
   */
  def compress(origin: Array[Byte]): Array[Byte]

  /**
   * 解压单个值
   *
   * @param compressed   已压缩的数据
   * @param originLength 原始长度
   * @return 解压后的数据
   */
  def decompress(compressed: Array[Byte], originLength: Int): Array[Byte]
}

object SsTableCompressor {
  private val log = LoggerFactory.getLogger(this.getClass)

  def recover(rawDict: Array[Byte]): SsTableCompressor = if (rawDict.isEmpty) SsTableCompressor.DEFAULT else {
    val dictType = rawDict.head
    dictType match
      case ZstdSsTableCompressor.DICT_TYPE =>
        val dict = rawDict.slice(1, rawDict.length)
        ZstdSsTableCompressor().loadDict(dict)
      case NoneSsTableCompressor.DICT_TYPE =>
        NoneSsTableCompressor
      case _ =>
        log.error("Unsupported SsTableCompressor type: {}", dictType)
        SsTableCompressor.DEFAULT
  }

  val DEFAULT = NoneSsTableCompressor
}
