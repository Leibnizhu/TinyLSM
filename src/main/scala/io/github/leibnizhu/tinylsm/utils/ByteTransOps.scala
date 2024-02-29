package io.github.leibnizhu.tinylsm.utils

object ByteTransOps {

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
