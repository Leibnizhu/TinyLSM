package io.github.leibnizhu.tinylsm.compact

import io.github.leibnizhu.tinylsm.TestUtils.*
import io.github.leibnizhu.tinylsm.compact.CompactionOptions.{FullCompaction, NoCompaction}
import io.github.leibnizhu.tinylsm.utils.Unbounded
import io.github.leibnizhu.tinylsm.{LsmStorageInner, TinyLsm}
import org.scalatest.funsuite.AnyFunSuite

import java.util.concurrent.TimeUnit

class FullCompactionTaskTest extends AnyFunSuite {

  test("week2_day1_task1_full_compaction") {
    val storage = LsmStorageInner(tempDir(), compactionOption(CompactionOptions.FullCompaction))
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

  test("week2_day1_task3_integration") {
    val storage = LsmStorageInner(tempDir(), compactionOption(CompactionOptions.FullCompaction))
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
      entry("0", "2333333"),
      entry("00", "2333"),
      entry("2", "2333"),
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

  test("week3_day2_compaction_integration") {
    val dir = tempDir()
    val options = compactionOption(NoCompaction, true)
    val storage = TinyLsm(dir, options)
    storage.newTxn()
    for (i <- 0 to 20000) {
      storage.put("0", "%02000d".format(i))
    }
    TimeUnit.SECONDS.sleep(1)
    while (!storage.inner.state.read(_.immutableMemTables.isEmpty)) {
      storage.inner.forceFlushNextImmutableMemTable()
    }
    assert(storage.inner.state.read(_.l0SsTables.length) > 1)
    storage.forceFullCompaction()
    storage.inner.dumpState()
    dumpFilesInDir(dir)
    assert(storage.inner.state.read(_.l0SsTables.isEmpty))
    assertResult(1)(storage.inner.state.read(_.levels.length))
    assertResult(1)(storage.inner.state.read(_.levels.head._2.length))

    for (i <- 0 to 100) {
      storage.put("1", "%02000d".format(i))
    }
    storage.inner.forceFreezeMemTable()
    TimeUnit.SECONDS.sleep(1)
    while (!storage.inner.state.read(_.immutableMemTables.isEmpty)) {
      storage.inner.forceFlushNextImmutableMemTable()
    }
    storage.forceFullCompaction()
    storage.inner.dumpState()
    dumpFilesInDir(dir)
    assert(storage.inner.state.read(_.l0SsTables.isEmpty))
    assertResult(1)(storage.inner.state.read(_.levels.length))
    assertResult(2)(storage.inner.state.read(_.levels.head._2.length))
  }
}
