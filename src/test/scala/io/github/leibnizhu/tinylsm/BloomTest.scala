package io.github.leibnizhu.tinylsm

import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable.ListBuffer
import scala.util.hashing.MurmurHash3

class BloomTest extends AnyFunSuite {

  private val keyNum: Int = 100

  private def keyOf(i: Int) = "key_" + "%03d".format(i * 5)

  private def valueOf(i: Int) = "value_" + "%03d".format(i)

  test("week1_day7_task1_bloom_filter") {
    val keyHashes = ListBuffer[Int]()
    for (idx <- 0 until keyNum) {
      val key = keyOf(idx)
      keyHashes += MurmurHash3.stringHash(key)
    }
    val bitsPerKey = Bloom.bloomBitsPerKey(keyHashes.length, 0.01)
    println(s"bits per key: $bitsPerKey")
    val bloom = Bloom(keyHashes.toList, bitsPerKey)
    println(s"bloom size: ${bloom.filter.size()}, hashFuncNum: ${bloom.hashFuncNum}")
    assert(bloom.hashFuncNum < 30)
    for (idx <- 0 until keyNum) {
      val key = keyOf(idx)
      assert(bloom.mayContains(MurmurHash3.stringHash(key)), s"should contains ${key}")
    }

    var x = 0
    var cnt = 0
    for (idx <- keyNum until keyNum * 1000) {
      if (bloom.mayContains(MurmurHash3.stringHash(keyOf(idx)))) {
        x += 1
      }
      cnt += 1
    }
    println(s"total key: $cnt, may match key: $x, falsePositiveRate: ${"%1.4f".format(x.toDouble/cnt)}")
    assert(x != cnt)
    assert(x != 0)
  }
}
