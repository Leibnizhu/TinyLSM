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

  private def generateBlock(): Block = {
    val builder = new BlockBuilder(10000)
    for (i <- 0 until 100) {
      val key = "key_" + "%03d".format(i)
      val value = "value_" + "%03d".format(i)
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

  }

  test("week1_day3_task2_block_seek_key") {

  }


}
