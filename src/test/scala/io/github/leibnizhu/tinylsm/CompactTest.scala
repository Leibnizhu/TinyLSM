package io.github.leibnizhu.tinylsm

import io.github.leibnizhu.tinylsm.TestUtils.*
import org.scalatest.funsuite.AnyFunSuite

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
}
