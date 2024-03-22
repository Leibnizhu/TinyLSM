package io.github.leibnizhu.tinylsm.utils

import io.github.leibnizhu.tinylsm.utils.ByteTransOps.{bytesToInt, low2BytesToInt, bytesToLong}

class ByteArrayReader(val bytes: Array[Byte]) {
  var curPos = 0

  /**
   * @return 剩余未读的byte数
   */
  def remaining: Int = bytes.length - curPos

  def seekTo(offset: Int): ByteArrayReader = {
    curPos = offset
    this
  }

  def readUint16(): Int = {
    low2BytesToInt(readByte(), readByte())
  }

  def readUint32(): Int = {
    bytesToInt(readByte(), readByte(), readByte(), readByte())
  }
  
  def readUint64(): Long = {
    bytesToLong(readByte(), readByte(), readByte(), readByte(),readByte(), readByte(), readByte(), readByte())
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

  def readTailUint16(): Int = {
    low2BytesToInt(bytes(bytes.length - 2), bytes.last)
  }

  def readTailUnit32(): Int = {
    bytesToInt(bytes.slice(bytes.length - 4, bytes.length))
  }
}
