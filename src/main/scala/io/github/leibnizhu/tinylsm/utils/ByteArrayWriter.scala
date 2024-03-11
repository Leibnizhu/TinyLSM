package io.github.leibnizhu.tinylsm.utils

import io.github.leibnizhu.tinylsm.MemTableKey
import io.github.leibnizhu.tinylsm.utils.ByteTransOps.{intLow2Bytes, intToByteArray, longToByteArray}

import scala.collection.mutable.ArrayBuffer

class ByteArrayWriter {
  def this(size: Int) = {
    this()
    reserve(size)
  }

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

  def putUint64(l: Long): ByteArrayWriter = {
    buffer.appendAll(longToByteArray(l))
    this
  }

  def putBytes(bytes: Array[Byte]): ByteArrayWriter = {
    buffer.appendAll(bytes)
    this
  }
  
  def putKey(key: MemTableKey): ByteArrayWriter = {
    this.putBytes(key.bytes).putUint64(key.ts)
  }

  def putByte(byte: Byte): ByteArrayWriter = {
    buffer.append(byte)
    this
  }

}


