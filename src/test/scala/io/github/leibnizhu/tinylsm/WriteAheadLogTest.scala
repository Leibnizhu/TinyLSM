package io.github.leibnizhu.tinylsm

import io.github.leibnizhu.tinylsm.TestUtils.{compactionOption, dumpFilesInDir, tempDir}
import io.github.leibnizhu.tinylsm.compact.CompactionOptions
import io.github.leibnizhu.tinylsm.compact.CompactionOptions.{LeveledCompactionOptions, SimpleCompactionOptions, TieredCompactionOptions}
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.util

class WriteAheadLogTest extends AnyFunSuite {

  test("week2_day6_wal_encode_decode") {
    val walFile = new File(tempDir(), System.currentTimeMillis() + ".wal")
    val wal = WriteAheadLog(walFile)
    for (i <- 0 until 200) {
      wal.put("key_%03d".format(i).getBytes, "value_%03d".format(i).getBytes)
    }
    wal.sync()
    Thread.sleep(100)

    val map = new util.HashMap[ByteArrayKey, MemTableValue]()
    val recovered = WriteAheadLog(walFile).recover(map)
    for (i <- 0 until 200) {
      assertResult("value_%03d".format(i))(new String(map.get(ByteArrayKey("key_%03d".format(i).getBytes))))
    }
  }

  test("week2_day6_integration_leveled") {
    manifestIntegrationTest(LeveledCompactionOptions(
      levelSizeMultiplier = 2,
      level0FileNumCompactionTrigger = 2,
      maxLevels = 3,
      baseLevelSizeMb = 1
    ))
  }

  test("week2_day6_integration_tiered") {
    manifestIntegrationTest(TieredCompactionOptions(
      maxSizeAmplificationPercent = 200,
      sizeRatio = 1,
      minMergeWidth = 3,
      numTiers = 3
    ))
  }

  test("week2_day6_integration_simple") {
    manifestIntegrationTest(SimpleCompactionOptions(
      sizeRatioPercent = 200,
      level0FileNumCompactionTrigger = 2,
      maxLevels = 3
    ))
  }

  private def manifestIntegrationTest(options: CompactionOptions): Unit = {
    val rootDir = tempDir()
    val storage = TinyLsm(rootDir, compactionOption(options, true))
    for (i <- 0 to 20) {
      storage.put("0", s"v$i")
      if (i % 2 == 0) {
        storage.put("1", s"v$i")
      } else {
        storage.delete("1")
      }
      if (i % 2 == 1) {
        storage.put("2", s"v$i")
      } else {
        storage.delete("2")
      }
      storage.inner.forceFreezeMemTable()
    }
    storage.inner.dumpState()
    storage.close()

    // 部分sst未flush
    assert(storage.inner.state.memTable.nonEmpty || storage.inner.state.immutableMemTables.nonEmpty)
    storage.inner.dumpState()

    dumpFilesInDir(rootDir)

    // 用WAL恢复LSM
    val recovered = TinyLsm(rootDir, compactionOption(options, true))
    assertResult("v20")(recovered.get("0").get)
    assertResult("v20")(recovered.get("1").get)
    assert(recovered.get("2").isEmpty)
  }
}
