package io.github.leibnizhu.tinylsm.utils

import io.github.leibnizhu.tinylsm.{bytesToInt, low2BytesToInt}

class ByteArrayReader(val bytes: Array[Byte]) {
  private var curPos = 0;

  /**
   * @return 剩余未读的byte数
   */
  def remaining: Int = bytes.length - curPos

  def readUint16(): Int = {
    low2BytesToInt(readByte(), readByte())
  }

  def readUint32(): Int = {
    bytesToInt(readByte(), readByte(), readByte(), readByte())
  }

  def readByte(): Byte = {
    val byte = bytes(curPos)
    curPos += 1
    byte
  }

  def readBytes(length: Int): Array[Byte] = {
    val read = bytes.slice(curPos, curPos + length)
    curPos += length
    read
  }

}
