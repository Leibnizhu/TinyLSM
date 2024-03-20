package io.github.leibnizhu.tinylsm

import io.github.leibnizhu.tinylsm.block.{Block, BlockBuilder, BlockCache, BlockMeta}
import io.github.leibnizhu.tinylsm.iterator.*
import io.github.leibnizhu.tinylsm.utils.ByteTransOps.bytesToInt
import io.github.leibnizhu.tinylsm.utils.{Bloom, ByteArrayReader, ByteArrayWriter, FileObject}
import org.slf4j.LoggerFactory

import java.io.*
import scala.collection.mutable.ArrayBuffer
import scala.util.hashing.MurmurHash3

/**
 * SST包括多个DataBlock和一个Index
 * SST文件一般 256MB
 * SST结构
 * ---------------------------------------------------------------------------------------------------------
 * |      Block Section      |          Meta Section        |            Bloom Section           |  Extra  |
 * ---------------------------------------------------------------------------------------------------------
 * |   data  | ... |   data  | metadata | meta block offset | bloom filter | bloom filter offset |         |
 * | block 1 |     | block N |  varlen  |         u32       |    varlen    |        u32          |         |
 * ---------------------------------------------------------------------------------------------------------
 */
class SsTable(val file: FileObject,
              private val id: Int,
              val blockMeta: Array[BlockMeta],
              private val blockMetaOffset: Int,
              private val blockCache: Option[BlockCache],
              val firstKey: MemTableKey,
              val lastKey: MemTableKey,
              val bloom: Option[Bloom],
              // SST存储的最大时间戳
              val maxTimestamp: Long = 0) {

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
    Block.decode(blockData)
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
    partitionPoint(blockMeta, meta => meta.firstKey.compareTo(targetKey) <= 0)
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
  def mayContainsKey(key: MemTableKey): Boolean = {
    val keyInRange = firstKey.compareOnlyKeyTo(key) <= 0 && key.compareOnlyKeyTo(lastKey) <= 0
    if (keyInRange) {
      if (bloom.isDefined) {
        // 如果有布隆过滤器，则以布隆过滤器为准（说存在只是可能存在，说不存在是肯定不存在）
        bloom.get.mayContains(key.keyHash())
      } else {
        // 没有布隆过滤器，则以key范围为准
        true
      }
    } else {
      false
    }
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
    // 参考 SsTableBuilder.build ，最后是bloom的offset,先读bloom，再读meta
    val bloomOffset = bytesToInt(file.read(len - 4, 4))
    val rawBloom = file.read(bloomOffset, len - 4 - bloomOffset)
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
      maxTimestamp = maxTs
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
      maxTimestamp = 0
    )
  }
}

/**
 * 用于构建 SsTable
 *
 * @param blockSize Block大小
 */
class SsTableBuilder(val blockSize: Int) {
  private val log = LoggerFactory.getLogger(this.getClass)
  // 当前Block的builder
  private var builder = BlockBuilder(blockSize)
  // 当前Block的第一个和最后一个Key
  private var firstKey: Option[MemTableKey] = None
  private var lastKey: Option[MemTableKey] = None
  private val data: ByteArrayWriter = new ByteArrayWriter()
  var meta: ArrayBuffer[BlockMeta] = new ArrayBuffer()
  private val keyHashes: ArrayBuffer[Int] = new ArrayBuffer()
  private var maxTs: Long = 0

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
  private def finishBlock(): Unit = {
    // 新建Builder并交换
    val prevBuilder = builder
    builder = BlockBuilder(blockSize)
    // 也可以用B+树，而非排序的block
    val encodedBlock = prevBuilder.build().encode()
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

    // meta写入buffer
    val buffer = data
    val metaOffset = buffer.length
    BlockMeta.encode(meta, buffer, maxTs)
    buffer.putUint32(metaOffset)

    //  bloom 写入 buffer
    val bloom = Bloom(keyHashes.toArray, Bloom.bloomBitsPerKey(keyHashes.length, 0.01))
    val bloomOffset = buffer.length
    bloom.encode(buffer)
    buffer.putUint32(bloomOffset)

    // 生成sst文件
    val file = FileObject.create(path, buffer.toArray)
    log.info(s"Created new SST file: ${file.file.get.getName}")
    new SsTable(
      file = file,
      id = id,
      blockMeta = meta.toArray,
      blockMetaOffset = metaOffset,
      blockCache = blockCache,
      firstKey = meta.head.firstKey.copy(),
      lastKey = meta.last.lastKey.copy(),
      bloom = Some(bloom),
      maxTimestamp = 0
    )
  }
}