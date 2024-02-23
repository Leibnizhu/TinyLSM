package io.github.leibnizhu.tinylsm

import org.scalatest.funsuite.AnyFunSuite

class BlockTest extends AnyFunSuite {

  test("week1_day3_task1_block_build_single_key") {
    val builder = new BlockBuilder(16)
    assert(builder.add("233".getBytes, "233333".getBytes))
    val block = builder.build()
    block
  }

  test("week1_day3_task1_block_build_full") {
    val builder = new BlockBuilder(16)
    // 用了 2+2+2+2+2=10 byte
    assert(builder.add("11".getBytes, "11".getBytes))
    // 这里也许要 10byte，size=16是不够的
    assert(!builder.add("22".getBytes, "22".getBytes))
    val block = builder.build()
    block
  }

  test("week1_day3_task1_block_build_large_1") {
    val builder = new BlockBuilder(16)
    // 虽然超过16byte，但是允许第一个key超的，否则这个key永远不能写入
    assert(builder.add("11".getBytes, ("1" * 100).getBytes))
    builder.build()
  }

  test("week1_day3_task1_block_build_large_2") {
    val builder = new BlockBuilder(16)
    assert(builder.add("11".getBytes, "1".getBytes))
    assert(!builder.add("11".getBytes, ("1" * 100).getBytes))
    builder.build()
  }

  test("week1_day3_task1_block_build_all") {
    val block = generateBlock()
    block
  }

  private val keyNum: Int = 100

  private def keyOf(i: Int) = "key_" + "%03d".format(i * 5)

  private def valueOf(i: Int) = "value_" + "%03d".format(i)

  private def generateBlock(): Block = {
    val builder = new BlockBuilder(10000)
    for (i <- 0 to keyNum) {
      val key = keyOf(i)
      val value = valueOf(i)
      assert(builder.add(key.getBytes, value.getBytes))
    }
    builder.build()
  }

  test("week1_day3_task1_block_encode") {
    val block = generateBlock()
    block.encode()
  }

  test("week1_day3_task1_block_decode") {
    val block = generateBlock()
    val encoded = block.encode()
    val decodedBlock = Block.decode(encoded)
    assertResult(block.offsets)(decodedBlock.offsets)
    assertResult(block.data)(decodedBlock.data)
  }

  test("week1_day3_task1_small_block_decode") {
    val builder = new BlockBuilder(32)
    builder.add("11".getBytes, "11".getBytes)
    builder.add("22".getBytes, "22".getBytes)
    val block = builder.build()
    val encoded = block.encode()
    val decodedBlock = Block.decode(encoded)
    assertResult(block.offsets)(decodedBlock.offsets)
    assertResult(block.data)(decodedBlock.data)
  }

  test("week1_day3_task2_block_iterator") {
    val block = generateBlock()
    val iter = BlockIterator.createAndSeekToFirst(block)
    for (_ <- 0 to 5) {
      for (i <- 0 to keyNum) {
        val key = iter.key()
        val value = iter.value()
        assertResult(keyOf(i).getBytes)(key)
        assertResult(valueOf(i).getBytes)(value)
        iter.next()
      }
      iter.seekToFirst()
    }
  }

  test("week1_day3_task2_block_seek_key") {
    val block = generateBlock()
    val iter = BlockIterator.createAndSeekToKey(block, keyOf(0).getBytes)
    for (offset <- 1 to 5) {
      for (i <- 0 to keyNum) {
        val key = new String(iter.key())
        val value = new String(iter.value())
        assertResult(keyOf(i))(key)
        assertResult(valueOf(i))(value)
        // seekToKey 的逻辑是跳到>=指定key的位置，所以offset 1~5的范围内都是跳到同样的 keyOf(i+1) 对应位置
        iter.seekToKey(("key_" + "%03d".format(i * 5 + offset)).getBytes)
      }
      iter.seekToKey("k".getBytes)
    }
  }
}
