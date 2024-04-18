package io.github.leibnizhu.tinylsm

import io.github.leibnizhu.tinylsm.TestUtils.{TS_ENABLED, checkIteratorWithTs, tempDir}
import io.github.leibnizhu.tinylsm.block.{BlockCache, BlockIterator}
import io.github.leibnizhu.tinylsm.compress.{SsTableCompressor, ZstdSsTableCompressor}
import io.github.leibnizhu.tinylsm.iterator.*
import io.github.leibnizhu.tinylsm.utils.FileObject
import org.scalatest.Entry
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
    val builder = SsTableBuilder(16, SsTableCompressor.none())
    builder.add("233", "233333")
    val sstFile = prepareSstFile()
    builder.build(0, None, sstFile)
    assert(sstFile.exists())
  }

  test("week1_day4_task1_sst_build_two_blocks") {
    val builder = SsTableBuilder(16, SsTableCompressor.none())
    builder.add("11", "11")
    builder.add("22", "22")
    builder.add("33", "11")
    builder.add("44", "22")
    builder.add("55", "11")
    builder.add("66", "22")
    assert(builder.meta.length >= 2)
    val sstFile = prepareSstFile()
    builder.build(0, None, sstFile)
    assert(sstFile.exists())
  }

  private def generateSst(): SsTable = {
    val builder = SsTableBuilder(128, SsTableCompressor.none())
    for (i <- 0 until keyNum) {
      val key = keyOf(i)
      val value = valueOf(i)
      builder.add(key, value)
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
    assertResult(keyOf(0).getBytes)(newSst.firstKey.bytes)
    assertResult(keyOf(keyNum - 1).getBytes)(newSst.lastKey.bytes)

    val secondBlock = newSst.readBlock(1)
    val blockItr = BlockIterator(secondBlock)
    // 每个block 存了5条，所以7应该在第二个block，seekToKey能直接定位到7的key
    blockItr.seekToKey(MemTableKey.applyForTest(keyOf(7)))
    assertResult(keyOf(7))(new String(blockItr.key().bytes))
    assertResult(valueOf(7))(new String(blockItr.value()))
    blockItr.prev()
    assertResult(keyOf(6))(new String(blockItr.key().bytes))
    assertResult(valueOf(6))(new String(blockItr.value()))
  }

  test("week1_day4_task2_sst_iterator") {
    val sst = generateSst()
    val iterator = SsTableIterator.createAndSeekToFirst(sst)
    for (_ <- 0 until 5) {
      for (i <- 0 until keyNum) {
        val key = iterator.key()
        val value = iterator.value()
        assertResult(keyOf(i))(new String(key.bytes))
        assertResult(valueOf(i))(new String(value))
        iterator.next()
      }
      iterator.seekToFirst()
    }
  }

  test("week1_day4_task1_sst_seek_key") {
    val sst = generateSst()
    val iterator = SsTableIterator.createAndSeekToKey(sst, MemTableKey.applyForTest(keyOf(0)))
    for (offset <- 1 to 5) {
      for (i <- 0 until keyNum) {
        val key = iterator.key()
        val value = iterator.value()
        assertResult(keyOf(i))(new String(key.bytes))
        assertResult(valueOf(i))(new String(value))
        iterator.seekToKey(MemTableKey.applyForTest("key_" + "%03d".format(i * 5 + offset)))
      }
      iterator.seekToKey(MemTableKey.applyForTest("k"))
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
    // 据观察，没开启的时候是20个block
    // key都是  key_xxx，压缩后，每个key多了2byte记录前缀长度，少了4-6个前缀byte，估算 ((4+6)/2 -2)/7
    if (TS_ENABLED) {
      assert(sst.blockMeta.length <= 34)
    } else {
      assert(sst.blockMeta.length <= 25)
    }
  }

  test("week2_day1_sst_build_multi_version_simple") {
    val builder = new SsTableBuilder(16, SsTableCompressor.none())
    builder.add(MemTableKey("233".getBytes, 233), "233333".getBytes)
    builder.add(MemTableKey("233".getBytes, 0), "2333333".getBytes)
    val sstFile = new File(tempDir(), System.currentTimeMillis() + ".sst")
    builder.build(0, None, sstFile)
  }

  test("week2_day1_test_sst_build_multi_version_hard") {
    val sstFile = new File(tempDir(), System.currentTimeMillis() + ".sst")
    val builder = new SsTableBuilder(128, SsTableCompressor.none())
    val data = (0 until 100).map(id => {
      val key = MemTableKey("key%05d".format(id / 5).getBytes, 5 - id % 5)
      val value = "value%05d".format(id).getBytes
      Entry(key, value)
    }).toList
    data.foreach(e => builder.add(e.key, e.value))
    val builtSst = builder.build(1, None, sstFile)
    val readSst = SsTable.open(1, None, FileObject.open(sstFile))
    checkIteratorWithTs(data, SsTableIterator.createAndSeekToFirst(readSst))
  }
}
