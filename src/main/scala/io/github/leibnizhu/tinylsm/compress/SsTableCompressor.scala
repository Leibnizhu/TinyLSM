package io.github.leibnizhu.tinylsm.compress

import com.github.blemale.scaffeine.Scaffeine
import io.github.leibnizhu.tinylsm.block.{Block, BlockMeta}
import io.github.leibnizhu.tinylsm.compress.CompressState.{Compress, Decompress, Train}
import io.github.leibnizhu.tinylsm.compress.CompressorOptions.{Lz4, Zlib, Zstd}
import io.github.leibnizhu.tinylsm.compress.SsTableCompressor.none
import io.github.leibnizhu.tinylsm.iterator.SsTableIterator
import io.github.leibnizhu.tinylsm.utils.{ByteArrayWriter, PrefixBloom}
import io.github.leibnizhu.tinylsm.{MemTableValue, SsTable, SsTableBuilder}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

/**
 * SSTable的value压缩器
 * 生命周期应该是跟着SSTable走，序列化到SST文件，可以从SST文件恢复
 * 其中如果有动态字典，也应该是针对当前SST（的多个Block）的value
 */
trait SsTableCompressor extends AutoCloseable {
  private val log = LoggerFactory.getLogger(this.getClass)
  private[compress] var state: CompressState = Train

  def changeState(newState: CompressState): SsTableCompressor = {
    state = newState
    this
  }

  def isState(specState: CompressState): Boolean = state == specState

  def needTrainDict(): Boolean

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
   * @param blockSize block预估大小
   * @param blockData 原始block数据
   * @param blocks    未压缩的block们
   * @param meta      BlockMeta们
   * @return (已压缩的block数据, 与压缩后block数据匹配的Array[BlockMeta])
   */
  def compressSsTable(blockSize: Int, blockData: ByteArrayWriter,
                      blocks: ArrayBuffer[Block], meta: Array[BlockMeta],
                      prefixBloom: PrefixBloom): (ByteArrayWriter, Array[BlockMeta]) =
    if (needTrainDict()) {
      // 如果需要训练字典，那么之前生成的sst是没压缩的，需要重新遍历这个未完成的sst的数据，重新压缩生成新的block和meta数据
      val sstId = -1
      val blockCache = Scaffeine().maximumSize(meta.length).build[(Int, Int), Block]()
      blocks.zipWithIndex.foreach((block, idx) => {
        //      block.compressor = none(Decompress)
        blockCache.put((sstId, idx), block)
      })
      val sst = SsTable(
        file = null,
        id = sstId,
        blockMeta = meta,
        blockMetaOffset = 0,
        blockCache = Some(blockCache),
        firstKey = meta.head.firstKey.copy(),
        lastKey = meta.last.lastKey.copy(),
        bloom = None,
        prefixBloom = None,
        maxTimestamp = 0,
        // 前面没压缩，可以直接读
        compressor = none(Decompress)
      )
      val sstIter = SsTableIterator.createAndSeekToFirst(sst)
      this.changeState(Compress)
      val newSstBuilder = SsTableBuilder(blockSize, this, prefixBloom)
      while (sstIter.isValid) {
        newSstBuilder.add(sstIter.key(), sstIter.value())
        sstIter.next()
      }
      newSstBuilder.finishBlock()
      log.info("Completed SST compression. Before compression: {} Blocks, {} KB; After compression: {} Blocks, {} KB",
        blocks.length, "%.2f".format(blockData.length / 1024.0),
        newSstBuilder.meta.length, "%.2f".format(newSstBuilder.data.length / 1024.0))
      (newSstBuilder.data, newSstBuilder.meta.toArray)
    } else {
      (blockData, meta)
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

  def recover(rawDict: Array[Byte]): SsTableCompressor = (if (rawDict.isEmpty) NoneSsTableCompressor() else {
    val dictType = rawDict.head
    dictType match
      case ZstdSsTableCompressor.DICT_TYPE =>
        val dict = rawDict.slice(1, rawDict.length)
        ZstdSsTableCompressor().loadDict(dict)
      case ZlibSsTableCompressor.DICT_TYPE =>
        ZlibSsTableCompressor()
      case NoneSsTableCompressor.DICT_TYPE =>
        NoneSsTableCompressor()
      case Lz4SsTableCompressor.DICT_TYPE =>
        Lz4SsTableCompressor()
      case _ =>
        log.error("Unsupported SsTableCompressor type: {}", dictType)
        NoneSsTableCompressor()
  }).changeState(CompressState.Decompress)

  def create(options: CompressorOptions): SsTableCompressor = options match
    case Zstd(trainDict, sampleSize, dictSize, level) => ZstdSsTableCompressor(trainDict, sampleSize, dictSize, level)
    case Zlib(level) => ZlibSsTableCompressor(level)
    case Lz4(level) => Lz4SsTableCompressor(level)
    case CompressorOptions.None => NoneSsTableCompressor()

  def none(initState: CompressState = Train): SsTableCompressor = NoneSsTableCompressor().changeState(initState)
}
