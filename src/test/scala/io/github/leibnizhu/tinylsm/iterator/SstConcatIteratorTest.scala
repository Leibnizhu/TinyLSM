package io.github.leibnizhu.tinylsm.iterator

import io.github.leibnizhu.tinylsm.TestUtils.tempDir
import io.github.leibnizhu.tinylsm.{SsTable, SsTableBuilder}
import org.scalatest.funsuite.AnyFunSuite

import java.io.File

class SstConcatIteratorTest extends AnyFunSuite {


  test("week2_day1_task2_concat_iterator") {
    val lsmDir = tempDir()
    val ssTables = (1 to 10).map(i => generateConcatSst(i * 10, (i + 1) * 10, lsmDir, i)).toList
    for (key <- 0 until 120) {
      val iter = SstConcatIterator.createAndSeekToKey(ssTables, "%05d".format(key).getBytes)
      if (key < 10) {
        assert(iter.isValid)
        assertResult("00010")(new String(iter.key()))
      } else if (key >= 110) {
        assert(!iter.isValid)
      } else {
        assert(iter.isValid)
        assertResult("%05d".format(key))(new String(iter.key()))
      }
    }

    val iter = SstConcatIterator.createAndSeekToFirst(ssTables)
    assert(iter.isValid)
    assertResult("00010")(new String(iter.key()))
  }

  private def generateConcatSst(startKey: Int, endKey: Int, dir: File, id: Int): SsTable = {
    val builder = SsTableBuilder(128)
    for (idx <- startKey until endKey) {
      builder.add("%05d".format(idx), "test")
    }
    builder.build(0, None, new File(dir, id + ".sst"))
  }
}
