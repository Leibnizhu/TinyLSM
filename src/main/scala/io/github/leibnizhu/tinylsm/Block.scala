package io.github.leibnizhu.tinylsm


import com.github.blemale.scaffeine.{Cache, Scaffeine}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.*

/**
 * Block 结构：
 * ----------------------------------------------------------------------------------------------------
 * |             Data Section             |              Offset Section             |      Extra      |
 * ----------------------------------------------------------------------------------------------------
 * | Entry #1 | Entry #2 | ... | Entry #N | Offset #1 | Offset #2 | ... | Offset #N | num_of_elements(2B) |
 * ----------------------------------------------------------------------------------------------------
 * 每个Entry的结构：
 * -----------------------------------------------------------------------
 * |                           Entry #1                            | ... |
 * -----------------------------------------------------------------------
 * | key_len (2B) | key (keylen) | value_len (2B) | value (varlen) | ... |
 * -----------------------------------------------------------------------
 */
class Block(val data: Array[Byte], val offsets: Array[Int]) {

  /**
   * 将当前Block编码为byte数组
   *
   * @return
   */
  def encode(): Array[Byte] = {
    val buffer = new ArrayBuffer[Byte]()
    buffer.appendAll(data)
    offsets.map(intLow2Bytes).foreach(buffer.appendAll)
    buffer.appendAll(intLow2Bytes(offsets.length))
    buffer.toArray
  }

  def getFirstKey(): MemTableKey = {
    val buffer = ByteArrayReader(data)
    val overlapLen = buffer.readUint16()
    assert(overlapLen == 0)
    val keyLen = buffer.readUint16()
    buffer.readBytes(keyLen)
  }
}

object Block {

  /**
   * 将byte数组解码为Block，覆盖当前Block
   *
   * @param bytes byte数组
   */
  def decode(bytes: Array[Byte]): Block = {
    val byteLen = bytes.length
    val numOfElement = if (bytes.last < 0) (bytes.last + 256) else bytes.last.toInt
    val offsetBytes = bytes.slice(bytes.length - numOfElement * SIZE_OF_U16 - 2, bytes.length - 2)
    val offsetIntArray = offsetBytes.sliding(2, 2).map(tb => low2BytesToInt(tb(0), tb(1))).toArray
    val dataBytes = bytes.slice(0, bytes.length - numOfElement * SIZE_OF_U16 - 2)
    Block(dataBytes, offsetIntArray)
  }
}

class BlockBuilder(val blockSize: Int) {
  // 序列化的Entry数据
  val data: ArrayBuffer[Byte] = new ArrayBuffer()
  // 直接放offset，虽然入参是Int，其实需要的是无符号short，写入磁盘时按2byte
  val offsets: ArrayBuffer[Int] = new ArrayBuffer()
  // Block中第一个key
  private var firstKey: Option[MemTableKey] = None

  /**
   * 增加一个kv对
   *
   * @param key   key
   * @param value value
   * @return 是否添加成功
   */
  def add(key: MemTableKey, value: MemTableKey): Boolean = {
    // 基础验证，key非空、预估体积也不能超过 blockSize
    assert(key != null && key.nonEmpty, "key must not be empty")
    // 一条数据会增加 记录key长度的2byte、key本身，记录value长度的2byte、value本身、记录offset的2byte，所以乘以3
    // 这里加入了非空的前置条件，因为如果一个kv超过BlockSize，没有非空的前置条件的话，这个kv是永远无法写入
    // 也就是说Block里第一个kv是允许超过BlockSize的
    if (!isEmpty && estimatedSize() + key.length + value.length + SIZE_OF_U16 * 3 > blockSize) {
      return false
    }
    // 显然，新数据的offset就是当前data长度
    offsets += data.length

    // overlap 格式
    // key_overlap_len (u16) | rest_key_len (u16) | key (rest_key_len)
    // 当前key与firstKey的共同前缀byte数量
    val overlap = commonPrefix(key)
    // key_overlap_len
    data.appendAll(intLow2Bytes(overlap))
    //  rest_key_len (u16)
    data.appendAll(intLow2Bytes(key.length - overlap))
    // key内容
    data.appendAll(key.slice(overlap, key.length))
    // value的长度
    data.appendAll(intLow2Bytes(value.length))
    // value内容
    data.appendAll(value)

    if (firstKey.isEmpty) {
      firstKey = Some(key)
    }
    true
  }

  /**
   * @param key 指定key
   * @return 指定key与 firstKey 有多少个相同的前缀byte
   */
  private def commonPrefix(key: MemTableKey): Int = {
    if (firstKey.isEmpty) {
      return 0
    }
    var index = 0
    while (index < firstKey.get.length && index < key.length && firstKey.get(index) == key(index)) {
      index += 1
    }
    index
  }

  /**
   * @return 按data和offsets估算的体积
   */
  private def estimatedSize(): Int = {
    // data 已经是序列化的byte数据，所以直接算长度
    data.length +
      // offset部分
      offsets.length * SIZE_OF_U16 +
      // Extra部分
      SIZE_OF_U16
  }

  /**
   * @return 当前Builder是否为空
   */
  def isEmpty: Boolean = {
    offsets.isEmpty
  }

  /**
   * 构建 Block
   *
   * @return Block对象
   */
  def build(): Block = {
    if (isEmpty) {
      throw new IllegalStateException("block should not be empty")
    }
    Block(data.toArray, offsets.toArray)
  }
}

class BlockIterator(block: Block) extends MemTableStorageIterator {
  private var index: Int = 0;
  /**
   * 当前迭代到的key，初始化和迭代完之后都是 None
   */
  private var curKey: Option[MemTableKey] = None
  /**
   * 当前value在Block中data的下标
   */
  private var curValuePos: (Int, Int) = (0, 0)
  private val firstKey = block.getFirstKey()

  def seekToFirst(): Unit = {
    seekToIndex(0)
  }

  /**
   * 跳到 >=指定key的位置
   *
   * @param key 指定定位的key，
   */
  def seekToKey(key: MemTableKey): Unit = {
    // key是有序存储的，可以用二分法
    var low = 0
    var high = block.offsets.length
    while (low < high) {
      val mid = low + (high - low) / 2
      seekToIndex(mid)
      assert(isValid)
      val compare = byteArrayCompare(curKey.get, key)
      if (compare < 0) {
        low = mid + 1
      } else if (compare > 0) {
        high = mid
      } else {
        return
      }
    }
    seekToIndex(low)
  }

  override def key(): MemTableKey = {
    assert(isValid, "BlockIterator is invalid")
    curKey.orNull
  }

  override def value(): MemTableValue = {
    assert(isValid, "BlockIterator is invalid")
    block.data.slice(curValuePos._1, curValuePos._2)
  }

  override def isValid: Boolean = {
    curKey.isDefined
  }

  override def next(): Unit = {
    index += 1
    seekToIndex(index)
  }

  /**
   * 跳到上一个
   */
  def prev(): Unit = {
    // index == 0 时不能再往前跳，这算异常吗？还是直接跳过忽略？
    if (index > 0) {
      index -= 1
      seekToIndex(index)
    }
  }

  private def seekToIndex(index: Int): Unit = {
    if (index < 0) {
      throw new IllegalArgumentException("Index must be positive!")
    }
    if (index >= block.offsets.length) {
      // 越界，则不可用
      curKey = None
      curValuePos = (0, 0)
      return
    }

    // 根据 offset 段获取entry位置
    val entryOffset = block.offsets(index)
    // 先后读取overlap长度、剩余key长度、剩余key、value长度
    val overlapLength = low2BytesToInt(block.data(entryOffset), block.data(entryOffset + 1))
    val restKeyLength = low2BytesToInt(block.data(entryOffset + 2), block.data(entryOffset + 3))
    curKey = Some(firstKey.slice(0, overlapLength) ++
      block.data.slice(entryOffset + 4, entryOffset + 4 + restKeyLength))
    val valueOffset = entryOffset + 4 + restKeyLength
    val valueLength = low2BytesToInt(block.data(valueOffset), block.data(valueOffset + 1))
    curValuePos = (valueOffset + 2, valueOffset + 2 + valueLength)
    this.index = index
  }
}

object BlockIterator {

  def createAndSeekToFirst(block: Block): BlockIterator = {
    val itr = new BlockIterator(block)
    itr.seekToFirst()
    itr
  }

  def createAndSeekToKey(block: Block, key: MemTableKey): BlockIterator = {
    val itr = new BlockIterator(block)
    itr.seekToKey(key)
    itr
  }

}

type BlockCache = Cache[(Int, Int), Block]

object BlockCache {

  def apply(maxSize: Int, expire: FiniteDuration = 10.minute): BlockCache = {
    Scaffeine()
      .recordStats()
      .expireAfterWrite(expire)
      .maximumSize(maxSize)
      .build()
  }
}