package io.github.leibnizhu.tinylsm.utils

import io.github.leibnizhu.tinylsm.{Key, LsmStorageOptions}

import scala.collection.mutable.ArrayBuffer
import scala.util.hashing.MurmurHash3

/**
 * 前缀布隆过滤器
 *
 * @param prefixLen 如果非None，则截取指定长度的key作为前缀
 * @param delimiter 如果非None，则按指定分隔符切割key获得多个前缀
 */
class PrefixBloom(prefixLen: Option[Int], delimiter: Option[Byte]) {
  private val keyHashes: ArrayBuffer[Int] = new ArrayBuffer()

  def addKey(key: Key): Unit = {
    prefixLen.map(key.bytes.take(_)).map(bs => MurmurHash3.seqHash(bs)).foreach(keyHashes.addOne)
    delimiter.map(ByteTransOps.arrayPrefixes(key.bytes, _).map(MurmurHash3.seqHash(_)))
      .foreach(hashes => keyHashes.addAll(hashes))
  }

  def bloom(): Bloom = {
    val bitsPerKey = Bloom.bloomBitsPerKey(keyHashes.length, PrefixBloom.bloomFPRate)
    Bloom(keyHashes.toArray, bitsPerKey)
  }
}

object PrefixBloom {
  private val bloomFPRate: Double = 0.01

  def fromConfig(options: LsmStorageOptions): PrefixBloom =
    new PrefixBloom(options.prefixBloomLength, options.prefixBloomDelimiter)

  def empty(): PrefixBloom =
    new PrefixBloom(None, None)
}