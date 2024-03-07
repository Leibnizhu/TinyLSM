package io.github.leibnizhu.tinylsm

import io.github.leibnizhu.tinylsm.block.{Block, BlockBuilder, BlockCache}
import io.github.leibnizhu.tinylsm.iterator.*
import io.github.leibnizhu.tinylsm.utils.ByteTransOps.bytesToInt
import io.github.leibnizhu.tinylsm.utils.{ByteArrayReader, ByteArrayWriter}
import org.slf4j.LoggerFactory

import java.io.*
import java.nio.ByteBuffer
import java.util
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
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
              val maxTimestamp: Long = -1L) {

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
    partitionPoint(blockMeta, meta => util.Arrays.compare(meta.firstKey, targetKey) <= 0)
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
    val keyInRange = util.Arrays.compare(firstKey, key) <= 0 && util.Arrays.compare(key, lastKey) <= 0
    if (keyInRange) {
      if (bloom.isDefined) {
        // 如果有布隆过滤器，则以布隆过滤器为准（说存在只是可能存在，说不存在是肯定不存在）
        bloom.get.mayContains(byteArrayHash(key))
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
      print(s"${new String(itr.key())} => ${new String(itr.value())}, ")
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
    val blockMeta = BlockMeta.decode(rawMeta)

    // 构建sst
    new SsTable(
      file = file,
      id = id,
      blockMeta = blockMeta,
      blockMetaOffset = metaOffset,
      blockCache = blockCache,
      firstKey = blockMeta.head.firstKey.clone,
      lastKey = blockMeta.last.lastKey.clone,
      bloom = Some(bloomFilter),
      maxTimestamp = 0
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
  private val log = LoggerFactory.getLogger(classOf[SsTableBuilder])
  // 当前Block的builder
  private var builder = BlockBuilder(blockSize)
  // 当前Block的第一个和最后一个Key
  private var firstKey: Option[MemTableKey] = None
  private var lastKey: Option[MemTableKey] = None
  private val data: ByteArrayWriter = new ByteArrayWriter()
  var meta: ArrayBuffer[BlockMeta] = new ArrayBuffer()
  private val keyHashes: ArrayBuffer[Int] = new ArrayBuffer()

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
    keyHashes.addOne(byteArrayHash(key))
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

  def add(key: String, value: String): Unit = add(key.getBytes, value.getBytes)

  /**
   * 一个Block写完、满了后，的处理
   */
  private def finishBlock(): Unit = {
    // 新建Builder并交换
    val prevBuilder = builder
    builder = BlockBuilder(blockSize)
    // 也可以用B+树，而非排序的block
    val encodedBlock = prevBuilder.build().encode()
    meta.addOne(new BlockMeta(data.length, firstKey.get.clone(), lastKey.get.clone()))
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
    BlockMeta.encode(meta, buffer)
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
      firstKey = meta.head.firstKey.clone(),
      lastKey = meta.last.lastKey.clone(),
      bloom = Some(bloom),
      // TODO
      maxTimestamp = 0
    )
  }
}

class BlockMeta(
                 // DataBlock的偏移量
                 val offset: Int,
                 // DataBlock的第一个key
                 val firstKey: MemTableKey,
                 // DataBlock的最后一个key
                 val lastKey: MemTableKey
               ) {
  private def canEqual(other: Any): Boolean = other.isInstanceOf[BlockMeta]

  override def equals(other: Any): Boolean = other match
    case that: BlockMeta =>
      that.canEqual(this) &&
        offset == that.offset &&
        (firstKey sameElements that.firstKey) &&
        (lastKey sameElements that.lastKey)
    case _ => false

  override def hashCode(): Int =
    val state = Seq(offset, firstKey, lastKey)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)

  override def toString = s"BlockMeta($offset: ${new String(firstKey)} => ${new String(lastKey)})"
}

object BlockMeta {
  /**
   * 将BlockMeta编码写入Buffer
   *
   * @param buffer 要写入的buffer
   */
  def encode(blockMetas: ArrayBuffer[BlockMeta], buffer: ByteArrayWriter): Unit = {
    // meta序列化长度计算
    // 先存储meta的个数，Int
    var estimateSize: Int = SIZE_OF_INT
    for (meta <- blockMetas) {
      // offset, Int
      estimateSize += SIZE_OF_INT
      // firstKey 的长度
      estimateSize += SIZE_OF_U16
      // firstKey 实际内容
      estimateSize += meta.firstKey.length
      // lastKey 的长度
      estimateSize += SIZE_OF_U16
      // lastKey 实际内容
      estimateSize += meta.lastKey.length
    }
    // 最后的hash
    estimateSize += SIZE_OF_INT
    // 预先给 ArrayBuffer 扩容
    buffer.reserve(estimateSize)
    val metaOffset = buffer.length

    // 开始将meta内容写入buffer，先写入meta个数，然后是每个meta
    buffer.putUint32(blockMetas.length)
    for (meta <- blockMetas) {
      buffer.putUint32(meta.offset)
      buffer.putUint16(meta.firstKey.length)
      buffer.putBytes(meta.firstKey)
      buffer.putUint16(meta.lastKey.length)
      buffer.putBytes(meta.lastKey)
    }
    val metasCheckSum = MurmurHash3.seqHash(buffer.slice(metaOffset + SIZE_OF_INT, buffer.length))
    buffer.putUint32(metasCheckSum)

    assert(estimateSize == buffer.length - metaOffset)
  }

  /**
   * 从buffer读取数据、解码成BlockMeta
   *
   * @param bytes 要读取的byte数组
   * @return 解码出来的BlockMeta
   */
  def decode(bytes: Array[Byte]): Array[BlockMeta] = {
    val blockMetas = new ArrayBuffer[BlockMeta]()
    val buffer = ByteArrayReader(bytes)
    val metaLength = buffer.readUint32()
    // 实际byte的哈希
    val checkSum = MurmurHash3.seqHash(bytes.slice(SIZE_OF_INT, bytes.length - SIZE_OF_INT))
    for (i <- 0 until metaLength) {
      // 按写入顺序读取
      val offset = buffer.readUint32()
      val firstKeyLen = buffer.readUint16()
      val firstKey = buffer.readBytes(firstKeyLen)
      val lasKeyLen = buffer.readUint16()
      val lastKey = buffer.readBytes(lasKeyLen)
      blockMetas.addOne(BlockMeta(offset, firstKey, lastKey))
    }
    // 校验hash
    if (buffer.readUint32() != checkSum) {
      throw new IllegalStateException("Block meta checksum mismatched!!!")
    }
    blockMetas.toArray
  }
}

case class FileObject(file: Option[File], size: Long) {
  def read(offset: Long, length: Long): Array[Byte] = {
    if (file.isEmpty) {
      throw new IllegalArgumentException("FileObject cannot read file when file is not set")
    }
    val accessFile = new RandomAccessFile(file.get, "r")
    accessFile.seek(offset)
    val buffer = new Array[Byte](length.intValue)
    val readBytes = accessFile.read(buffer)
    if (readBytes < length) {
      buffer.slice(0, readBytes)
    } else {
      buffer
    }
  }

}

object FileObject {
  def create(path: File, data: Array[Byte]): FileObject = {
    if (!path.exists()) {
      path.createNewFile()
    }
    val writer = new BufferedOutputStream(new FileOutputStream(path))
    writer.write(data)
    writer.close()
    FileObject(Some(path), data.length)
  }

  def open(path: File): FileObject = {
    if (!path.exists()) {
      throw new IllegalArgumentException("File does not exists: " + path.getAbsolutePath)
    }
    FileObject(Some(path), path.length())
  }
}