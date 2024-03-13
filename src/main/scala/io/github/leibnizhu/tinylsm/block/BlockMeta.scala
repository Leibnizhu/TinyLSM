package io.github.leibnizhu.tinylsm.block

import io.github.leibnizhu.tinylsm.utils.{ByteArrayReader, ByteArrayWriter}
import io.github.leibnizhu.tinylsm.{MemTableKey, SIZE_OF_INT, SIZE_OF_LONG, SIZE_OF_U16}

import scala.collection.mutable.ArrayBuffer
import scala.util.hashing.MurmurHash3

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
        (firstKey.equals(that.firstKey)) &&
        (lastKey.equals(that.lastKey))
    case _ => false

  override def hashCode(): Int =
    val state = Seq(offset, firstKey, lastKey)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)

  override def toString = s"BlockMeta($offset: $firstKey => $lastKey)"
}

object BlockMeta {
  /**
   * 将BlockMeta编码写入Buffer
   *
   * @param blockMetas 要编码的BlockMeta数组
   * @param buffer     要写入的buffer
   * @param maxTs      当前block中最大时间戳
   */
  def encode(blockMetas: ArrayBuffer[BlockMeta], buffer: ByteArrayWriter, maxTs: Long): Unit = {
    // meta序列化长度计算
    // 先存储meta的个数，Int
    var estimateSize: Int = SIZE_OF_INT
    for (meta <- blockMetas) {
      // offset, Int
      estimateSize += SIZE_OF_INT
      // firstKey 的长度
      estimateSize += SIZE_OF_U16
      // firstKey 实际内容 + 时间戳
      estimateSize += meta.firstKey.rawLength
      // lastKey 的长度
      estimateSize += SIZE_OF_U16
      // lastKey 实际内容 + 时间戳
      estimateSize += meta.lastKey.rawLength
    }
    // 最大的timestamp
    estimateSize += SIZE_OF_LONG
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
      buffer.putKey(meta.firstKey)
      buffer.putUint16(meta.lastKey.length)
      buffer.putKey(meta.lastKey)
    }
    buffer.putUint64(maxTs)
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
  def decode(bytes: Array[Byte]): (Array[BlockMeta], Long) = {
    val blockMetas = new ArrayBuffer[BlockMeta]()
    val buffer = ByteArrayReader(bytes)
    val metaLength = buffer.readUint32()
    // 实际byte的哈希
    val checkSum = MurmurHash3.seqHash(bytes.slice(SIZE_OF_INT, bytes.length - SIZE_OF_INT))
    for (i <- 0 until metaLength) {
      // 按写入顺序读取
      val offset = buffer.readUint32()
      val firstKeyLen = buffer.readUint16()
      val firstKey = MemTableKey(buffer.readBytes(firstKeyLen), buffer.readUint64())
      val lasKeyLen = buffer.readUint16()
      val lastKey = MemTableKey(buffer.readBytes(lasKeyLen), buffer.readUint64())
      blockMetas.addOne(BlockMeta(offset, firstKey, lastKey))
    }
    val maxTs = buffer.readUint64()
    // 校验hash
    if (buffer.readUint32() != checkSum) {
      throw new IllegalStateException("Block meta checksum mismatched!!!")
    }
    (blockMetas.toArray, maxTs)
  }
}