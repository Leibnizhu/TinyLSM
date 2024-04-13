package io.github.leibnizhu.tinylsm.compress

import io.github.leibnizhu.tinylsm.TestUtils.tempDir
import io.github.leibnizhu.tinylsm.block.{BlockCache, BlockIterator}
import io.github.leibnizhu.tinylsm.{MemTableKey, SsTable, SsTableBuilder}
import org.scalatest.funsuite.AnyFunSuite

import java.io.File

class SsTableCompressTest extends AnyFunSuite {

  private val compressKeyNum: Int = 10000

  private def compressKeyOf(i: Int) = "key_" + "%0200d".format(i * 5)

  private def compressValueOf(i: Int) = "value_" + "%0200d".format(i)

  private def prepareSstFile(): File = {
    val sstFile = new File(tempDir(), "1.sst")
    if (sstFile.exists()) {
      sstFile.delete()
    }
    sstFile
  }

  private def generateCompressedSst(compressor: SsTableCompressor): SsTable = {
    val builder = SsTableBuilder(10240, compressor)
    for (i <- 0 until compressKeyNum) {
      val key = compressKeyOf(i)
      val value = compressValueOf(i)
      builder.add(key, value)
    }
    val sstFile = prepareSstFile()
    println("SST file: " + sstFile.getAbsolutePath)
    val ssTable = builder.build(0, Some(BlockCache(10)), sstFile)
    assert(sstFile.exists())
    ssTable
  }

  test("zstd_compressed_with_dict_sst_decode") {
    val sst = generateCompressedSst(new ZstdSsTableCompressor())
    val newSst = SsTable.open(0, None, sst.file)
    assertResult(sst.blockMeta)(newSst.blockMeta)
    assertResult(compressKeyOf(0).getBytes)(newSst.firstKey.bytes)
    assertResult(compressKeyOf(compressKeyNum - 1).getBytes)(newSst.lastKey.bytes)

    val firstBlock = newSst.readBlock(0)
    val blockItr1 = BlockIterator(firstBlock)
    blockItr1.seekToKey(MemTableKey.applyForTest(compressKeyOf(0)))
    assertResult(compressKeyOf(0))(new String(blockItr1.key().bytes))
    assertResult(compressValueOf(0))(new String(blockItr1.value()))

    val secondBlock = newSst.readBlock(1)
    val blockItr2 = BlockIterator(secondBlock)
    // 每个block 存了200+条，所以250应该在第二个block，seekToKey能直接定位到250的key
    blockItr2.seekToKey(MemTableKey.applyForTest(compressKeyOf(250)))
    assertResult(compressKeyOf(250))(new String(blockItr2.key().bytes))
    assertResult(compressValueOf(250))(new String(blockItr2.value()))
  }

  test("zstd_compressed_without_dict_sst_decode") {
    val sst = generateCompressedSst(new ZstdSsTableCompressor(trainDict = false))
    val newSst = SsTable.open(0, None, sst.file)
    assertResult(sst.blockMeta)(newSst.blockMeta)
    assertResult(compressKeyOf(0).getBytes)(newSst.firstKey.bytes)
    assertResult(compressKeyOf(compressKeyNum - 1).getBytes)(newSst.lastKey.bytes)

    val firstBlock = newSst.readBlock(0)
    val blockItr1 = BlockIterator(firstBlock)
    blockItr1.seekToKey(MemTableKey.applyForTest(compressKeyOf(0)))
    assertResult(compressKeyOf(0))(new String(blockItr1.key().bytes))
    assertResult(compressValueOf(0))(new String(blockItr1.value()))

    val secondBlock = newSst.readBlock(1)
    val blockItr2 = BlockIterator(secondBlock)
    // 每个block 存了200+条，所以250应该在第二个block，seekToKey能直接定位到250的key
    blockItr2.seekToKey(MemTableKey.applyForTest(compressKeyOf(250)))
    assertResult(compressKeyOf(250))(new String(blockItr2.key().bytes))
    assertResult(compressValueOf(250))(new String(blockItr2.value()))
  }

  test("zlib_compressed_sst_decode") {
    val sst = generateCompressedSst(new ZlibSsTableCompressor())
    val newSst = SsTable.open(0, None, sst.file)
    assertResult(sst.blockMeta)(newSst.blockMeta)
    assertResult(compressKeyOf(0).getBytes)(newSst.firstKey.bytes)
    assertResult(compressKeyOf(compressKeyNum - 1).getBytes)(newSst.lastKey.bytes)

    val firstBlock = newSst.readBlock(0)
    val blockItr1 = BlockIterator(firstBlock)
    blockItr1.seekToKey(MemTableKey.applyForTest(compressKeyOf(0)))
    assertResult(compressKeyOf(0))(new String(blockItr1.key().bytes))
    assertResult(compressValueOf(0))(new String(blockItr1.value()))

    val secondBlock = newSst.readBlock(1)
    val blockItr2 = BlockIterator(secondBlock)
    // 每个block 存了200+条，所以250应该在第二个block，seekToKey能直接定位到250的key
    blockItr2.seekToKey(MemTableKey.applyForTest(compressKeyOf(250)))
    assertResult(compressKeyOf(250))(new String(blockItr2.key().bytes))
    assertResult(compressValueOf(250))(new String(blockItr2.value()))
  }

  test("lz4_compressed_sst_decode") {
    val sst = generateCompressedSst(new Lz4SsTableCompressor())
    val newSst = SsTable.open(0, None, sst.file)
    assertResult(sst.blockMeta)(newSst.blockMeta)
    assertResult(compressKeyOf(0).getBytes)(newSst.firstKey.bytes)
    assertResult(compressKeyOf(compressKeyNum - 1).getBytes)(newSst.lastKey.bytes)

    val firstBlock = newSst.readBlock(0)
    val blockItr1 = BlockIterator(firstBlock)
    blockItr1.seekToKey(MemTableKey.applyForTest(compressKeyOf(0)))
    assertResult(compressKeyOf(0))(new String(blockItr1.key().bytes))
    assertResult(compressValueOf(0))(new String(blockItr1.value()))

    val secondBlock = newSst.readBlock(1)
    val blockItr2 = BlockIterator(secondBlock)
    // 每个block 存了250 条，所以300应该在第二个block，seekToKey能直接定位到300的key
    blockItr2.seekToKey(MemTableKey.applyForTest(compressKeyOf(300)))
    assertResult(compressKeyOf(300))(new String(blockItr2.key().bytes))
    assertResult(compressValueOf(300))(new String(blockItr2.value()))
  }

  test("zstd_lack_of_sample") {
    val compressor = new ZstdSsTableCompressor()
    val builder = SsTableBuilder(10240, compressor)
    for (i <- 0 until 3) {
      val key = compressKeyOf(i)
      val value = compressValueOf(i)
      builder.add(key, value)
    }
    val sstFile = prepareSstFile()
    // 会有报错日志，样本太少，训练字典失败，但是能运行成功
    val ssTable = builder.build(0, Some(BlockCache(10)), sstFile)
    assert(sstFile.exists())
    val firstBlock = ssTable.readBlock(0)
    val blockItr1 = BlockIterator(firstBlock)
    blockItr1.seekToKey(MemTableKey.applyForTest(compressKeyOf(2)))
    assertResult(compressKeyOf(2))(new String(blockItr1.key().bytes))
    assertResult(compressValueOf(2))(new String(blockItr1.value()))
  }
}
