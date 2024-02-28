package io.github.leibnizhu.tinylsm

import io.github.leibnizhu.tinylsm.utils.ByteArrayWriter
import io.github.leibnizhu.tinylsm.{bytesToInt}

import java.util
import scala.util.boundary
import scala.util.hashing.MurmurHash3

class Bloom(val filter: util.BitSet, val hashFuncNum: Int) {

  def mayContains(h: Int): Boolean =
    // hash函数个数太多，已经很难判定
    if (hashFuncNum > 30) true else {
      // 布隆过滤器的总位数
      val nBits = filter.size()
      var hash = h
      // 每次hash的偏移量
      val delta = (hash >> 17) | (hash << 15)
      boundary:
        for (_ <- 0 until hashFuncNum) {
          val bitPos = Math.abs(hash) % nBits
          if (!filter.get(bitPos)) {
            boundary.break(false)
          }
          hash += delta
        }
        true
    }

  /**
   * 序列化到指定buffer
   * 顺序：filter -> hashFuncNum -> hash(filter, hashFuncNum)
   *
   * @param buffer 指定buffer
   */
  def encode(buffer: ByteArrayWriter): Unit = {
    val offset = buffer.length
    buffer.putByteArray(filter.toByteArray)
    // hashFuncNum 最大30，可以放入一个byte
    buffer.putByte(hashFuncNum.toByte)
    val checksum = MurmurHash3.seqHash(buffer.slice(offset, buffer.length))
    buffer.putUint32(checksum)
  }
}

object Bloom {

  private val ln2 = Math.log(2)

  def apply(hashes: Seq[Int], bitsPerKey: Int): Bloom = {
    val k = (bitsPerKey * 0.69).toInt.max(1).min(30)
    var nBits = (hashes.length * bitsPerKey).max(64)
    val filter = new util.BitSet(nBits)
    nBits = filter.size()
    for (h <- hashes) {
      var hash = h
      val delta = (hash >> 17) | (hash << 15);
      for (_ <- 0 until k) {
        val bitPos = Math.abs(hash) % nBits
        // 将hash对应bit置1
        filter.set(bitPos)
        hash += delta
      }
    }
    new Bloom(filter, k)
  }

  /**
   *
   *
   * @param entries           插入元素个数
   * @param falsePositiveRate 误报率
   * @return 布隆过滤器长度
   */
  def bloomBitsPerKey(entries: Int, falsePositiveRate: Double): Int = {
    // 布隆过滤器长度
    val size = -1.0 * entries.toDouble * Math.log(falsePositiveRate) / ln2 / ln2
    Math.ceil(size / entries).toInt
  }

  /**
   * 从byte数组还原Bloom
   *
   * @param bytes byte数组
   * @return Bloom对象
   */
  def decode(bytes: Array[Byte]): Bloom = {
    val checksum = bytesToInt(bytes.slice(bytes.length - 4, bytes.length))
    if (checksum != MurmurHash3.seqHash(bytes.slice(0, bytes.length - 4))) {
      throw new IllegalArgumentException("Bloom filter checksum mismatch")
    }
    val filter = bytes.slice(0, bytes.length - 5)
    val k = bytes(bytes.length - 5).toInt
    val bitSet = util.BitSet.valueOf(filter)
    new Bloom(bitSet, k)
  }
}
