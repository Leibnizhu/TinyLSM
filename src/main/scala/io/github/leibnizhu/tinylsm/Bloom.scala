package io.github.leibnizhu.tinylsm

import java.util
import scala.util.boundary

class Bloom(val filter: util.BitSet, val hashFuncNum: Int) {

  def mayContains(h: Int): Boolean =
    // hash函数个数太多，已经很难判定
    if (hashFuncNum > 30) true else {
      // 布隆过滤器的总位数
      val nBits = filter.length
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
}

object Bloom {

  private val ln2 = Math.log(2)

  def apply(hashes: Seq[Int], bitsPerKey: Int): Bloom = {
    val k = (bitsPerKey * 0.69).toInt.max(1).min(30)
    val nBits = (hashes.length * bitsPerKey).max(64)
    val filter = new util.BitSet(nBits)
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
}
