package io.github.leibnizhu.tinylsm.utils

import io.github.leibnizhu.tinylsm.{Key, LsmStorageOptions}

import scala.collection.mutable.ArrayBuffer
import scala.util.hashing.MurmurHash3


class PrefixBloom(prefixLen: Option[Int], delimiter: Option[Byte], bloom: Bloom) {

  def encode(buffer: ByteArrayWriter): Unit = {
    buffer.putBoolean(prefixLen.isDefined)
    buffer.putUint32(prefixLen.getOrElse(0))
    buffer.putBoolean(delimiter.isDefined)
    buffer.putByte(delimiter.getOrElse(0))
    bloom.encode(buffer)
  }

  /**
   * 假定 prefixLen 和 delimiter 配置不修改的情况
   *
   * @param prefix 当前要查询的前缀
   * @return
   */
  def notContainsPrefix(prefix: Key): Boolean = {
    // TODO
    if (prefixLen.isDefined) {
      // prefixBloomFilter 保存有当前sst所有key

    }
    if (delimiter.isDefined) {

    }
    false
  }
}

object PrefixBloom {

  def decode(bytes: Array[Byte]): PrefixBloom = {
    // 简单粗暴
    val prefixLen = if (bytes(0) == 1.toByte) {
      Some(ByteTransOps.bytesToInt(bytes(1), bytes(2), bytes(3), bytes(4)))
    } else {
      None
    }
    val delimiter = if (bytes(5) == 1.toByte) {
      Some(bytes(6))
    } else {
      None
    }
    val bloom = Bloom.decode(bytes.slice(7, bytes.length))
    new PrefixBloom(prefixLen, delimiter, bloom)
  }
}

/**
 * 前缀布隆过滤器
 *
 * @param prefixLen 如果非None，则截取指定长度的key作为前缀
 * @param delimiter 如果非None，则按指定分隔符切割key获得多个前缀
 */
class PrefixBloomBuilder(prefixLen: Option[Int], delimiter: Option[Byte]) {
  private val keyHashes: ArrayBuffer[Int] = new ArrayBuffer()

  def addKey(key: Key): Unit = {
    prefixLen.map(key.bytes.take(_)).map(bs => MurmurHash3.seqHash(bs)).foreach(keyHashes.addOne)
    delimiter.map(ByteTransOps.arrayPrefixes(key.bytes, _).map(MurmurHash3.seqHash(_)))
      .foreach(hashes => keyHashes.addAll(hashes))
  }

  def build(): PrefixBloom = {
    val bitsPerKey = Bloom.bloomBitsPerKey(keyHashes.length, PrefixBloomBuilder.bloomFPRate)
    val bloomFilter = Bloom(keyHashes.toArray, bitsPerKey)
    new PrefixBloom(prefixLen, delimiter, bloomFilter)
  }
}

object PrefixBloomBuilder {
  private val bloomFPRate: Double = 0.01

  def fromConfig(options: LsmStorageOptions): PrefixBloomBuilder =
    new PrefixBloomBuilder(options.prefixBloomLength, options.prefixBloomDelimiter)

  def empty(): PrefixBloomBuilder =
    new PrefixBloomBuilder(None, None)
}