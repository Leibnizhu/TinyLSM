package io.github.leibnizhu

import scala.util.hashing.MurmurHash3

package object tinylsm {
  // 16位无符号整型，u16，即short的无符号版，所占的byte数量
  val SIZE_OF_U16 = 2
  // Int的byte数
  val SIZE_OF_INT = 4

  // 已删除的key对应的value
  val DELETE_TOMBSTONE = Array[Byte]()

  type MemTableKey = Array[Byte]
  type MemTableValue = Array[Byte]
  type MemTableEntry = java.util.Map.Entry[ByteArrayKey, MemTableValue]
  type MemTableStorageIterator = StorageIterator[MemTableKey, MemTableValue]

  def byteArrayCompare(a: Array[Byte], b: Array[Byte]): Int = {
    if (a eq b) {
      return 0
    }
    if (a == null || b == null) {
      return if (a == null) -1 else 1
    }

    val i = byteArrayMismatch(a, b, Math.min(a.length, b.length))
    if (i >= 0) {
      return java.lang.Byte.compare(a(i), b(i))
    }
    a.length - b.length
  }

  def byteArrayMismatch(a: Array[Byte], b: Array[Byte], length: Int): Int = {
    var i: Int = 0
    while (i < length) {
      if (a(i) != b(i)) {
        return i
      }
      i += 1
    }
    -1
  }

  def byteArrayHash(bytes: Array[Byte]): Int = {
    MurmurHash3.seqHash(bytes)
  }


  /**
   * @param i 整型数字
   * @return 整型数字的低2位的byte，低位在后
   */
  def intLow2Bytes(i: Int): List[Byte] =
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

  def intToByteArray(i: Int): Array[Byte] = Array(
    ((i >> 24) & 0xFF).asInstanceOf[Byte],
    ((i >> 16) & 0xFF).asInstanceOf[Byte],
    ((i >> 8) & 0xFF).asInstanceOf[Byte],
    (i & 0xFF).asInstanceOf[Byte]
  )

  def bytesToInt(bytes: Array[Byte]): Int = {
    bytesToInt(bytes(0), bytes(1), bytes(2), bytes(3))
  }

  def bytesToInt(b3: Byte, b2: Byte, b1: Byte, b0: Byte): Int = {
    val safeB3: Int = if (b3 < 0) b3 + 256 else b3
    val safeB2: Int = if (b2 < 0) b2 + 256 else b2
    val safeB1: Int = if (b1 < 0) b1 + 256 else b1
    val safeB0: Int = if (b0 < 0) b0 + 256 else b0
    (safeB3 << 24) + (safeB2 << 16) + (safeB1 << 8) + safeB0
  }
}