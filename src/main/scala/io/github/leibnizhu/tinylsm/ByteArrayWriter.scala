package io.github.leibnizhu.tinylsm

import scala.collection.mutable.ArrayBuffer

class ByteArrayWriter {
  private val buffer = new ArrayBuffer[Byte]()

  def length: Int = buffer.length

  def reserve(size: Int): Unit = buffer.sizeHint(buffer.length + size)

  def toArray: Array[Byte] = buffer.toArray

  def slice(from: Int, until: Int): ArrayBuffer[Byte] = buffer.slice(from, until)

  def putUint16(i: Int): ByteArrayWriter = {
    buffer.appendAll(intLow2Bytes(i))
    this
  }

  def putUint32(i: Int): ByteArrayWriter = {
    buffer.appendAll(intToByteArray(i))
    this
  }

  def putByteArray(bytes: Array[Byte]): ByteArrayWriter = {
    buffer.appendAll(bytes)
    this
  }

  def putByte(byte: Byte): ByteArrayWriter = {
    buffer.append(byte)
    this
  }

}


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