package io.github.leibnizhu.tinylsm.block

import io.github.leibnizhu.tinylsm.utils.ByteArrayWriter
import io.github.leibnizhu.tinylsm.{MemTableKey, SIZE_OF_U16}

import scala.collection.mutable.ArrayBuffer

class BlockBuilder(val blockSize: Int) {
  // 序列化的Entry数据
  val data = new ByteArrayWriter()
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
    data.putUint16(overlap)
    //  rest_key_len (u16)
    data.putUint16(key.length - overlap)
    // key内容
    data.putBytes(key.slice(overlap, key.length))
    // value的长度
    data.putUint16(value.length)
    // value内容
    data.putBytes(value)

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
