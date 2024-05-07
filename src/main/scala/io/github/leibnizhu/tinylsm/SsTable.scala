package io.github.leibnizhu.tinylsm

import io.github.leibnizhu.tinylsm.block.{Block, BlockBuilder, BlockCache, BlockMeta}
import io.github.leibnizhu.tinylsm.compress.CompressState.Decompress
import io.github.leibnizhu.tinylsm.compress.SsTableCompressor
import io.github.leibnizhu.tinylsm.iterator.*
import io.github.leibnizhu.tinylsm.utils.*
import io.github.leibnizhu.tinylsm.utils.ByteTransOps.bytesToInt
import org.slf4j.LoggerFactory

import java.io.*
import scala.collection.immutable.ArraySeq
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.math.Ordering.Implicits.infixOrderingOps
import scala.util.hashing.MurmurHash3

/**
 * SST包括多个DataBlock和一个Index
 * SST文件一般 256MB
 * SST结构
 * --------------------------------------------------------------------------------------------------------------------------------------
 * |      Block Section      |          Meta Section        |            Bloom Section           |                   Dict               |
 * --------------------------------------------------------------------------------------------------------------------------------------
 * |   data  | ... |   data  | metadata | meta block offset | bloom filter | bloom filter offset |  dict type | dict data | dict offset |
 * | block 1 |     | block N |  varlen  |         u32       |    varlen    |        u32          |     1B     |   varlen  |     u32     |
 * --------------------------------------------------------------------------------------------------------------------------------------
 *
 * TODO 考虑其他SST编码结构，如增加二级索引 https://disc-projects.bu.edu/lethe/， 或B+ Tree
 */
class SsTable(val file: FileObject,
              private val id: Int,
              val blockMeta: Array[BlockMeta],
              private val blockMetaOffset: Int,
              private val blockCache: Option[BlockCache],
              val firstKey: MemTableKey,
              val lastKey: MemTableKey,
              val bloom: Option[Bloom],
              val prefixBloomFilter: Option[PrefixBloom],
              // SST存储的最大时间戳
              val maxTimestamp: Long = 0,
              val compressor: SsTableCompressor) {
  // SsTable创建后用于读取，compressor应该都是解压模式；只有生成的时候用到训练和压缩模式
  compressor.changeState(Decompress)

  def readBlock(blockIndex: Int): Block = {
    val blockOffset = blockMeta(blockIndex).offset
    // 如果是读最后一个block，那么接下来就是meta，可以用meta的offset；否则用下一个block的offset
    val blockEndOffset = if (blockIndex == blockMeta.length - 1)
      blockMetaOffset else blockMeta(blockIndex + 1).offset
    // 还要减掉最后的checkSum
    val blockLength = blockEndOffset - blockOffset - SIZE_OF_INT
    val blockDataWithChecksum = new ByteArrayReader(file.read(blockOffset, blockEndOffset - blockOffset))
    val blockData = blockDataWithChecksum.readBytes(blockLength)
    val checksum = blockDataWithChecksum.readUint32()
    // 校验hash
    if (MurmurHash3.seqHash(blockData) != checksum) {
      throw new IllegalStateException("Block data checksum mismatched!!!")
    }
    Block.decode(blockData, compressor)
  }

  /**
   * 使用 (sst_id, block_id) 作为缓存key
   *
   * @param blockIndex Block索引
   * @return Block
   */
  def readBlockCached(blockIndex: Int): Block = {
    if (blockCache.isDefined) {
      blockCache.get.get((id, blockIndex), (_, bIdx) => readBlock(bIdx))
    } else {
      readBlock(blockIndex)
    }
  }

  def findBlockIndex(targetKey: MemTableKey): Int = {
    // 二分查找，找到最后（数组的右边）一个 meta.firstKey <= targetKey 的 meta 的索引
    partitionPoint(ArraySeq.unsafeWrapArray(blockMeta), (meta: BlockMeta) => meta.firstKey.compareTo(targetKey) <= 0)
  }

  def numOfBlocks(): Int = blockMeta.length

  def tableSize(): Long = file.size

  def sstId(): Int = id

  /**
   * 除了firstKey lastKey这个范围命中以外，还进一步判断bloom过滤器，提高效率
   *
   * @param key 要判断的key
   * @return 这个key是否可能在当前sst里面
   */
  def mayContainsKey(key: Array[Byte]): Boolean = {
    val keyInRange = firstKey.bytes <= key && key <= lastKey.bytes
    if (keyInRange) {
      if (bloom.isDefined) {
        // 如果有布隆过滤器，则以布隆过滤器为准（说存在只是可能存在，说不存在是肯定不存在）
        bloom.get.mayContains(key)
      } else {
        // 没有布隆过滤器，则以key范围为准
        true
      }
    } else {
      false
    }
  }

  /**
   * 当前sst里面是否可能包含指定前缀的key
   *
   * @param prefix 要判断的前缀
   * @return 当前sst里面是否可能包含指定前缀的key
   */
  def mayContainsPrefix(prefix: Key): Boolean = {
    // prefix 超过当前sst的 lastkey，不可能包含这个前缀的key
    if (prefix.rawKey() > lastKey.rawKey()) {
      return false
    }
    // prefix 的下个值不到当前sst的 firstKey，不可能包含这个前缀的key
    prefix.prefixUpperEdge() match
      case Excluded(right: Key) if right.rawKey() <= firstKey.rawKey() => return false
      case _ =>

    // 3.按前缀bloom 切割前缀判定
    if (prefixBloomFilter.isDefined) {
      if (prefixBloomFilter.get.notContainsPrefix(prefix)) {
        return false
      }
    }
    true
  }

  /**
   * sst的范围是否包含用户指定的scan范围
   *
   * @param userBegin scan指定的左边界
   * @param userEnd   scan指定的右边界
   * @return sst是否满足scan范围
   */
  def containsRange(userBegin: Bound, userEnd: Bound): Boolean = {
    // 判断scan的右边界如果小于SST的最左边第一个key，那么这个sst肯定不包含这个scan范围
    userEnd match
      case Excluded(right: Key) if right.rawKey() <= firstKey.rawKey() => return false
      case Included(right: Key) if right.rawKey() < firstKey.rawKey() => return false
      case _ =>
    // 判断scan的左边界如果大于SST的最右边最后一个key，那么这个sst肯定不包含这个scan范围
    userBegin match
      case Excluded(left: Key) if left.rawKey() >= lastKey.rawKey() => return false
      case Included(left: Key) if left.rawKey() > lastKey.rawKey() => return false
      case _ =>
    true
  }

  def printSsTable(): Unit = {
    val itr = SsTableIterator.createAndSeekToFirst(this)
    print(s"SsTable(ID=$id) content: ")
    while (itr.isValid) {
      print(s"${new String(itr.key().bytes)}@${itr.key().ts} => ${new String(itr.value())}, ")
      itr.next()
    }
    println()
  }
}

object SsTable {
  def open(id: Int, blockCache: Option[BlockCache], file: FileObject): SsTable = {
    val len = file.size
    // 参考 SsTableBuilder.build ，最后是dict的offset,先读dict、然后bloom，再读meta
    val dictOffset = bytesToInt(file.read(len - 4, 4))
    val compressDict = file.read(dictOffset, len - 4 - dictOffset)
    val compressor = SsTableCompressor.recover(compressDict)

    val prefixBloomOffset = bytesToInt(file.read(dictOffset - 4, 4))
    val rawPrefixBloom = file.read(prefixBloomOffset, dictOffset - 4 - prefixBloomOffset)
    val prefixBloomFilter = PrefixBloom.decode(rawPrefixBloom)

    val bloomOffset = bytesToInt(file.read(prefixBloomOffset - 4, 4))
    val rawBloom = file.read(bloomOffset, prefixBloomOffset - 4 - bloomOffset)
    val bloomFilter = Bloom.decode(rawBloom)

    // 读meta，bloom开始再向前4byte就是meta的offset了
    val metaOffset = bytesToInt(file.read(bloomOffset - 4, 4))
    val rawMeta = file.read(metaOffset, bloomOffset - 4 - metaOffset)
    val (blockMeta, maxTs) = BlockMeta.decode(rawMeta)

    // 构建sst
    new SsTable(
      file = file,
      id = id,
      blockMeta = blockMeta,
      blockMetaOffset = metaOffset,
      blockCache = blockCache,
      firstKey = blockMeta.head.firstKey.copy(),
      lastKey = blockMeta.last.lastKey.copy(),
      bloom = Some(bloomFilter),
      prefixBloomFilter = Some(prefixBloomFilter),
      maxTimestamp = maxTs,
      compressor = compressor
    )
  }

  def createMetaOnly(id: Int, fileSize: Long, firstKey: MemTableKey, lastKey: MemTableKey): SsTable = {
    new SsTable(
      file = FileObject(None, fileSize),
      id = id,
      blockMeta = Array(),
      blockMetaOffset = 0,
      blockCache = None,
      firstKey = firstKey,
      lastKey = lastKey,
      bloom = None,
      prefixBloomFilter = None,
      maxTimestamp = 0,
      compressor = SsTableCompressor.none()
    )
  }
}

/**
 * 用于构建 SsTable
 *
 * @param blockSize Block大小
 */
class SsTableBuilder(val blockSize: Int, val compressor: SsTableCompressor,
                     val prefixBloomBuilder: PrefixBloomBuilder = PrefixBloomBuilder.empty()) {
  private val log = LoggerFactory.getLogger(this.getClass)
  // 当前Block的builder
  private var builder = BlockBuilder(blockSize, compressor)
  // 当前Block的第一个和最后一个Key
  private var firstKey: Option[MemTableKey] = None
  private var lastKey: Option[MemTableKey] = None
  private[tinylsm] val data: ByteArrayWriter = new ByteArrayWriter()
  var meta: ArrayBuffer[BlockMeta] = new ArrayBuffer()
  private val keyHashes: ArrayBuffer[Int] = new ArrayBuffer()
  private var maxTs: Long = 0
  private val blocks = ArrayBuffer[Block]()

  /**
   * 往SST增加一个kv对
   *
   * @param key   key
   * @param value value
   */
  def add(key: MemTableKey, value: MemTableValue): Unit = {
    if (firstKey.isEmpty) {
      firstKey = Some(key)
    }
    if (key.ts > maxTs) {
      maxTs = key.ts
    }
    keyHashes.addOne(key.keyHash())
    this.prefixBloomBuilder.addKey(key)
    // value字典采样
    if (compressor.needTrainDict()) {
      compressor.addDictSample(value)
    }
    // add可能因为BlockBuilder满了导致失败
    if (builder.add(key, value)) {
      lastKey = Some(key)
      return
    }
    //  到了这里即 BlockBuilder.add 失败了，是因为BlockBuilder满了，需要创建一个新的 BlockBuilder 并重新add
    finishBlock()
    assert(builder.add(key, value))
    //那么此时这个key是新的Block的第一个key
    firstKey = Some(key)
    lastKey = Some(key)
  }

  def add(key: String, value: String): Unit = add(MemTableKey(key.getBytes), value.getBytes)

  /**
   * 一个Block写完、满了后，的处理
   */
  private[tinylsm] def finishBlock(): Unit = {
    // 新建Builder并交换
    val curBlock = builder.build()
    blocks += curBlock
    builder = BlockBuilder(blockSize, compressor)
    // 也可以用B+树，而非排序的block
    val encodedBlock = curBlock.encode()
    meta.addOne(new BlockMeta(data.length, firstKey.get.copy(), lastKey.get.copy()))
    val checkSum = byteArrayHash(encodedBlock)
    data.putBytes(encodedBlock)
    data.putUint32(checkSum)
  }

  /**
   * 由于DataBlock远大于MetaBlock，作为估算的大小，可以直接返回data部分的大小
   *
   * @return SST的预估大小
   */
  def estimateSize(): Int = data.length

  def build(id: Int, blockCache: Option[BlockCache], path: File): SsTable = {
    // 剩余的数据作为一个Block
    finishBlock()

    // 生成压缩字典、对value执行压缩
    val dict = compressor.generateDict()
    val (buffer, finalMatas) = compressor.compressSsTable(blockSize, data, blocks, meta.toArray, prefixBloomBuilder)

    // meta写入buffer
    val metaOffset = buffer.length
    BlockMeta.encode(finalMatas, buffer, maxTs)
    buffer.putUint32(metaOffset)

    //  bloom 写入 buffer
    val bloomFilter = Bloom(keyHashes.toArray, Bloom.bloomBitsPerKey(keyHashes.length, 0.01))
    val bloomOffset = buffer.length
    bloomFilter.encode(buffer)
    buffer.putUint32(bloomOffset)

    // 前缀bloom 写入buffer
    val prefixBloomOffset = buffer.length
    val prefixBloom = prefixBloomBuilder.build()
    prefixBloom.encode(buffer)
    buffer.putUint32(prefixBloomOffset)

    // 压缩字典写入buffer
    val dictOffset = buffer.length
    buffer.putByte(compressor.DICT_TYPE)
    buffer.putBytes(dict)
    buffer.putUint32(dictOffset)

    // 生成sst文件
    val fileObj = FileObject.create(path, buffer.toArray)
    val file = fileObj.file.get
    log.info("Created new SST with {} file: {} {} KB", compressor, file.getName, "%.3f".format(file.length() / 1024.0))
    new SsTable(
      file = fileObj,
      id = id,
      blockMeta = finalMatas,
      blockMetaOffset = metaOffset,
      blockCache = blockCache,
      firstKey = finalMatas.head.firstKey.copy(),
      lastKey = finalMatas.last.lastKey.copy(),
      bloom = Some(bloomFilter),
      prefixBloomFilter = Some(prefixBloom),
      maxTimestamp = maxTs,
      compressor = compressor
    )
  }
}