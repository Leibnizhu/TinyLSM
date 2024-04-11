package io.github.leibnizhu.tinylsm.block

import io.github.leibnizhu.tinylsm.*
import io.github.leibnizhu.tinylsm.compress.SsTableCompressor
import io.github.leibnizhu.tinylsm.utils.{ByteArrayReader, ByteArrayWriter}

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
 * | key_overlap_len (2B) | remaining_key_len (2B) | key (remaining_key_len) | timestamp (8B) | value_len (2B) | value (varlen) | ... |
 * -----------------------------------------------------------------------
 */
class Block(val data: Array[Byte], val offsets: Array[Int], val compressor: SsTableCompressor = SsTableCompressor.DEFAULT) {

  /**
   * 将当前Block编码为byte数组
   *
   * @return
   */
  def encode(): Array[Byte] = {
    val buffer = new ByteArrayWriter()
    buffer.putBytes(data)
    offsets.foreach(buffer.putUint16)
    buffer.putUint16(offsets.length)
    buffer.toArray
  }

  def getFirstKey(): MemTableKey = {
    val buffer = ByteArrayReader(data)
    val overlapLen = buffer.readUint16()
    assert(overlapLen == 0)
    val keyLen = buffer.readUint16()
    val keyBytes = buffer.readBytes(keyLen)
    MemTableKey(keyBytes, buffer.readUint64())
  }
}

object Block {

  /**
   * 将byte数组解码为Block，覆盖当前Block
   *
   * @param bytes byte数组
   */
  def decode(bytes: Array[Byte], compressor: SsTableCompressor = SsTableCompressor.DEFAULT): Block = {
    val buffer = ByteArrayReader(bytes)
    val numOfElement = buffer.readTailUint16()
    // 总长度减去 所有offset + offset长度2B 就是要读的data长度
    val dataBytes = buffer.readBytes(bytes.length - numOfElement * SIZE_OF_U16 - SIZE_OF_U16)
    val offsetIntArray = (0 until numOfElement).map(_ => buffer.readUint16()).toArray
    Block(dataBytes, offsetIntArray, compressor)
  }
}