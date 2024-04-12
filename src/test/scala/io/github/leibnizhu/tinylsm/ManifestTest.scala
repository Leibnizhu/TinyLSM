package io.github.leibnizhu.tinylsm

import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.github.leibnizhu.tinylsm.TestUtils.{compactionOption, dumpFilesInDir, tempDir}
import io.github.leibnizhu.tinylsm.compact.*
import io.github.leibnizhu.tinylsm.compact.CompactionOptions.{LeveledCompactionOptions, SimpleCompactionOptions, TieredCompactionOptions}
import io.github.leibnizhu.tinylsm.compress.CompressorOptions
import org.scalatest.funsuite.AnyFunSuite

import java.io.File

class ManifestTest extends AnyFunSuite {

  test("json_test") {
    val record: ManifestRecord = ManifestCompaction(
      SimpleCompactionTask(None, List(1, 2, 3), 1, List(4, 5, 6), true), List(1)
    )
    val mapper = JsonMapper.builder()
      .addModule(DefaultScalaModule)
      .build()
    val json = mapper.writeValueAsString(record)
    println(json)
    val record1 = mapper.readValue(json, classOf[ManifestRecord])
    assertResult(record)(record1)
  }

  test("week2_day5_manifest_encode_decode_test") {
    val manifestFile = new File(tempDir(), System.currentTimeMillis() + "-MANIFEST")
    val manifest = new Manifest(manifestFile)
    val flush = ManifestFlush(123)
    manifest.addRecord(flush)
    val memtable = ManifestNewMemtable(456)
    manifest.addRecord(memtable)
    val fullCompact = ManifestCompaction(FullCompactionTask(List(1, 2, 3), List(4, 5, 6)), List(1))
    manifest.addRecord(fullCompact)
    val simpleCompact = ManifestCompaction(SimpleCompactionTask(None, List(1, 2, 3), 1, List(4, 5, 6), true), List(1))
    manifest.addRecord(simpleCompact)
    val tieredCompact = ManifestCompaction(TieredCompactionTask(List((1, List(6)), (2, List(4, 5)), (3, List(1, 2, 3))), true), List(1))
    manifest.addRecord(tieredCompact)
    val leveledCompact = ManifestCompaction(LeveledCompactionTask(None, List(1, 2, 3), 1, List(4, 5, 6), true), List(1))
    manifest.addRecord(leveledCompact)

    val records = manifest.recover()
    assertResult(6)(records.length)
    assertResult(List(
      flush, memtable, fullCompact, simpleCompact, tieredCompact, leveledCompact
    ))(records)

    manifestFile.delete()
  }

  test("week2_day5_integration_leveled") {
    manifestIntegrationTest(LeveledCompactionOptions(
      levelSizeMultiplier = 2,
      level0FileNumCompactionTrigger = 2,
      maxLevels = 3,
      baseLevelSizeMb = 1
    ))
  }

  test("week2_day5_integration_tiered") {
    manifestIntegrationTest(TieredCompactionOptions(
      maxSizeAmplificationPercent = 200,
      sizeRatio = 1,
      minMergeWidth = 3,
      numTiers = 3
    ))
  }

  test("week2_day5_integration_simple") {
    manifestIntegrationTest(SimpleCompactionOptions(
      sizeRatioPercent = 200,
      level0FileNumCompactionTrigger = 2,
      maxLevels = 3
    ))
  }

  private def manifestIntegrationTest(options: CompactionOptions): Unit = {
    val rootDir = tempDir()
    val storage = TinyLsm(rootDir, LsmStorageOptions(
      4096,
      1 << 20,
      1024,
      2,
      options,
      CompressorOptions.None,
      false,
      false))
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
      storage.forceFlush()
    }
    Thread.sleep(100)
    storage.inner.dumpState()
    storage.close()

    // 所有sst都被flush了
    assert(storage.inner.state.memTable.isEmpty)
    assert(storage.inner.state.immutableMemTables.isEmpty)
    storage.inner.dumpState()
    dumpFilesInDir(rootDir)

    // 使用manifest恢复LSM
    val recovered = TinyLsm(rootDir, compactionOption(options))
    assertResult("v20")(recovered.get("0").get)
    assertResult("v20")(recovered.get("1").get)
    assert(recovered.get("2").isEmpty)
  }
}
