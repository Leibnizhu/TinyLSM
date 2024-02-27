package io.github.leibnizhu.tinylsm

import io.github.leibnizhu.tinylsm.TestUtils.*
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import scala.collection.mutable.ListBuffer

class CompactTest extends AnyFunSuite {

  test("week2_day1_task1_full_compaction") {
    val storage = LsmStorageInner(tempDir(), doCompactionOption())
    storage.newTxn()
    storage.put("0", "v1")
    syncStorage(storage)
    storage.put("0", "v2")
    storage.put("1", "v2")
    storage.put("2", "v2")
    syncStorage(storage)
    storage.delete("0")
    storage.delete("2")
    syncStorage(storage)
    assertResult(3)(storage.state.read(_.l0SsTables.length))

    val iter1 = constructMergeIteratorOverStorage(storage.state)
    if (TS_ENABLED) {
      checkIterator(List(
        entry("0", ""),
        entry("0", "v2"),
        entry("0", "v1"),
        entry("1", "v2"),
        entry("2", ""),
        entry("2", "v2"),
      ), iter1)
    } else {
      checkIterator(List(
        entry("0", ""),
        entry("1", "v2"),
        entry("2", ""),
      ), iter1)

      storage.forceFullCompaction()
      assert(storage.state.read(_.l0SsTables.isEmpty))

      val iter2 = constructMergeIteratorOverStorage(storage.state)
      if (TS_ENABLED) {
        checkIterator(List(
          entry("0", ""),
          entry("0", "v2"),
          entry("0", "v1"),
          entry("1", "v2"),
          entry("2", ""),
          entry("2", "v2"),
        ), iter2)
      } else {
        checkIterator(List(
          entry("1", "v2"),
        ), iter2)
      }

      storage.put("0", "v3")
      storage.put("2", "v3")
      syncStorage(storage)
      storage.delete("1")
      syncStorage(storage)
      val iter3 = constructMergeIteratorOverStorage(storage.state)
      if (TS_ENABLED) {
        checkIterator(List(
          entry("0", "v3"),
          entry("0", ""),
          entry("0", "v2"),
          entry("0", "v1"),
          entry("1", ""),
          entry("1", "v2"),
          entry("2", "v3"),
          entry("2", ""),
          entry("2", "v2"),
        ), iter3)
      } else {
        checkIterator(List(
          entry("0", "v3"),
          entry("1", ""),
          entry("2", "v3"),
        ), iter3)
      }

      storage.forceFullCompaction()
      assert(storage.state.read(_.l0SsTables.isEmpty))
      val iter4 = constructMergeIteratorOverStorage(storage.state)
      if (TS_ENABLED) {
        checkIterator(List(
          entry("0", "v3"),
          entry("0", ""),
          entry("0", "v2"),
          entry("0", "v1"),
          entry("1", ""),
          entry("1", "v2"),
          entry("2", "v3"),
          entry("2", ""),
          entry("2", "v2"),
        ), iter4)
      } else {
        checkIterator(List(
          entry("0", "v3"),
          entry("2", "v3"),
        ), iter4)
      }
    }
  }

  private def doCompactionOption(): LsmStorageOptions = LsmStorageOptions(
    4096,
    2 << 20,
    50,
    CompactionOptions.SimpleCompactionOptions(0, 0, 0),
    false,
    false)

  private def generateConcatSst(startKey: Int, endKey: Int, dir: File, id: Int): SsTable = {
    val builder = SsTableBuilder(128)
    for (idx <- startKey until endKey) {
      builder.add("%05d".format(idx), "test")
    }
    builder.build(0, None, new File(dir, id + ".sst"))
  }

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

  test("week2_day1_task3_integration") {
    val storage = LsmStorageInner(tempDir(), doCompactionOption())
    storage.put("0", "2333333")
    storage.put("00", "2333333")
    storage.put("4", "23")
    syncStorage(storage)

    storage.delete("4")
    syncStorage(storage)

    storage.forceFullCompaction()
    assert(storage.state.l0SsTables.isEmpty)
    assert(storage.state.levels(0)._2.nonEmpty)

    storage.put("1", "233")
    storage.put("2", "2333")
    syncStorage(storage)

    storage.put("00", "2333")
    storage.put("3", "23333")
    storage.delete("1")
    syncStorage(storage)

    storage.forceFullCompaction()
    assert(storage.state.l0SsTables.isEmpty)
    assert(storage.state.levels(0)._2.nonEmpty)

    checkIterator(List(
      entry("0","2333333"),
      entry("00","2333"),
      entry("2","2333"),
      entry("3", "23333"),
    ), storage.scan(Unbounded(), Unbounded()))
    assertResult("2333333")(storage.get("0").get)
    assertResult("2333")(storage.get("00").get)
    assertResult("2333")(storage.get("2").get)
    assertResult("23333")(storage.get("3").get)
    assert(storage.get("4").isEmpty)
    assert(storage.get("--").isEmpty)
    assert(storage.get("555").isEmpty)
  }
}
