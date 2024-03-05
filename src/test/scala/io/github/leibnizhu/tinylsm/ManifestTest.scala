package io.github.leibnizhu.tinylsm

import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.github.leibnizhu.tinylsm.compact.SimpleCompactionTask
import org.scalatest.funsuite.AnyFunSuite

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
}
