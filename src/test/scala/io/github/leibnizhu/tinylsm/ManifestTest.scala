package io.github.leibnizhu.tinylsm

import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.github.leibnizhu.tinylsm.TestUtils.tempDir
import io.github.leibnizhu.tinylsm.compact.{FullCompactionTask, LeveledCompactionTask, SimpleCompactionTask, TieredCompactionTask}
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

  test("manifest_encode_decode_test") {
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
}
