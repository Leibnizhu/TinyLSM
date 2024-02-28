package io.github.leibnizhu.tinylsm.block

import io.github.leibnizhu.tinylsm.*
import io.github.leibnizhu.tinylsm.utils.ByteArrayReader

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