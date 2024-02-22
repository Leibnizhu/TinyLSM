package io.github.leibnizhu.tinylsm

import scala.collection.mutable.ArrayBuffer

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
    offsets.map(Block.getIntLow2Byte).foreach(buffer.appendAll)
    buffer.appendAll(Block.getIntLow2Byte(offsets.length))
    buffer.toArray
  }
}

object Block {
  // 16位无符号整型，u16，即short的无符号版，所占的byte数量
  val SIZE_OF_U16 = 2

  /**
   * 将byte数组解码为Block，覆盖当前Block
   *
   * @param bytes byte数组
   */
  def decode(bytes: Array[Byte]): Block = {
    val byteLen = bytes.length
    val numOfElement = bytes.last
    val offsetBytes = bytes.slice(bytes.length - numOfElement * Block.SIZE_OF_U16 - 2, bytes.length - 2)
    val offsetIntArray = offsetBytes.sliding(2, 2).map(tb => Block.low2BytesToInt(tb(0), tb(1))).toArray
    val dataBytes = bytes.slice(0, bytes.length - numOfElement * Block.SIZE_OF_U16 - 2)
    Block(dataBytes, offsetIntArray)
  }

  /**
   * @param i 整型数字
   * @return 整型数字的低2位的byte，低位在后
   */
  def getIntLow2Byte(i: Int): List[Byte] =
    List(((i >> 8) & 0xFF).asInstanceOf[Byte], (i & 0xFF).asInstanceOf[Byte])

  /**
   * @param high int的8-15位
   * @param low  int的0-7位
   * @return 两位byte拼接还原一个Int（实际取值范围为无符号short）
   */
  def low2BytesToInt(high: Byte, low: Byte): Int = {
    val safeHigh: Int = if (high < 0) high + 256 else high
    val safeLow: Int = if (low < 0) low + 256 else low
    (safeHigh << 8) + safeLow
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
    if (!isEmpty && estimatedSize() + key.length + value.length + Block.SIZE_OF_U16 * 3 > blockSize) {
      return false
    }
    // 显然，新数据的offset就是当前data长度
    offsets += data.length

    // key的长度
    data.appendAll(Block.getIntLow2Byte(key.length))
    // key内容
    data.appendAll(key)
    // value的长度
    data.appendAll(Block.getIntLow2Byte(value.length))
    // value内容
    data.appendAll(value)

    if (firstKey.isEmpty) {
      firstKey = Some(key)
    }
    true
  }


  /**
   * @return 按data和offsets估算的体积
   */
  private def estimatedSize(): Int = {
    // data 已经是序列化的byte数据，所以直接算长度
    data.length +
      // offset部分
      offsets.length * Block.SIZE_OF_U16 +
      // Extra部分
      Block.SIZE_OF_U16
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

class BlockIterator {
  def createAndSeekToFirst(): Unit = ???

  def createAndSeekToKey(): Unit = ???

  def key(): MemTableKey = ???

  def value(): MemTableValue = ???

  def isValid: Boolean = ???

  def seekToFirst(): Unit = ???

  def next(): Unit = ???

  def seekToKey(key: MemTableKey) = ???
}