package io.github.leibnizhu.tinylsm

import io.github.leibnizhu.tinylsm.TestUtils.tempDir
import org.scalatest.funsuite.AnyFunSuite

import java.io.File

class SsTableTest extends AnyFunSuite {

  private val keyNum: Int = 100

  private def keyOf(i: Int) = "key_" + "%03d".format(i * 5)

  private def valueOf(i: Int) = "value_" + "%03d".format(i)

  private def prepareSstFile(): File = {
    val sstFile = new File(tempDir(), "1.sst")
    if (sstFile.exists()) {
      sstFile.delete()
    }
    sstFile
  }

  test("week1_day4_task1_sst_build_single_key") {
    val builder = SsTableBuilder(16)
    builder.add("233".getBytes, "233333".getBytes)
    val sstFile = prepareSstFile()
    builder.build(0, None, sstFile)
    assert(sstFile.exists())
  }

  test("week1_day4_task1_sst_build_two_blocks") {
    val builder = SsTableBuilder(16)
    builder.add("11".getBytes, "11".getBytes)
    builder.add("22".getBytes, "22".getBytes)
    builder.add("33".getBytes, "11".getBytes)
    builder.add("44".getBytes, "22".getBytes)
    builder.add("55".getBytes, "11".getBytes)
    builder.add("66".getBytes, "22".getBytes)
    assert(builder.meta.length >= 2)
    val sstFile = prepareSstFile()
    builder.build(0, None, sstFile)
    assert(sstFile.exists())
  }

  private def generateSst(): SsTable = {
    val builder = SsTableBuilder(128)
    for (i <- 0 until keyNum) {
      val key = keyOf(i)
      val value = valueOf(i)
      builder.add(key.getBytes, value.getBytes)
    }
    val sstFile = prepareSstFile()
    println("SST file: " + sstFile.getAbsolutePath)
    val ssTable = builder.build(0, Some(BlockCache(10)), sstFile)
    assert(sstFile.exists())
    ssTable
  }

  test("week1_day4_task1_sst_build_all") {
    generateSst()
  }

  test("week1_day4_task1_sst_decode") {
    val sst = generateSst()
    val newSst = SsTable.open(0, None, sst.file)
    assertResult(sst.blockMeta)(newSst.blockMeta)
    assertResult(keyOf(0).getBytes)(newSst.firstKey)
    assertResult(keyOf(keyNum - 1).getBytes)(newSst.lastKey)

    val secondBlock = newSst.readBlock(1)
    val blockItr = BlockIterator(secondBlock)
    // 每个block 存了5条，所以7应该在第二个block，seekToKey能直接定位到7的key
    blockItr.seekToKey(keyOf(7).getBytes)
    assertResult(keyOf(7))(new String(blockItr.key()))
  }

  test("week1_day4_task2_sst_iterator") {
    val sst = generateSst()
    val iterator = SsTableIterator.createAndSeekToFirst(sst)
    for (_ <- 0 until 5) {
      for (i <- 0 until keyNum) {
        val key = iterator.key()
        val value = iterator.value()
        assertResult(keyOf(i))(new String(key))
        assertResult(valueOf(i))(new String(value))
        iterator.next()
      }
      iterator.seekToFirst()
    }
  }

  test("week1_day4_task1_sst_seek_key") {
    val sst = generateSst()
    val iterator = SsTableIterator.createAndSeekToKey(sst, keyOf(0).getBytes)
    for (offset <- 1 to 5) {
      for (i <- 0 until keyNum) {
        val key = iterator.key()
        val value = iterator.value()
        assertResult(keyOf(i))(new String(key))
        assertResult(valueOf(i))(new String(value))
        iterator.seekToKey(("key_" + "%03d".format(i * 5 + offset)).getBytes)
      }
      iterator.seekToKey("k".getBytes)
    }
  }

  test("week1_day7_task2_sst_decode") {
    val sst1 = generateSst()
    val sst2 = SsTable.open(0, None, FileObject.open(sst1.file.file.get))
    val bloom1 = sst1.bloom.get
    val bloom2 = sst2.bloom.get
    assertResult(bloom1.hashFuncNum)(bloom2.hashFuncNum)
    assertResult(bloom1.filter)(bloom2.filter)
  }

  test("week1_day7_task3_block_key_compression") {
    val sst = generateSst()
    assert(sst.blockMeta.length <= 25)
  }
}
