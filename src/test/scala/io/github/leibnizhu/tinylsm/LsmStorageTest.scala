package io.github.leibnizhu.tinylsm

import io.github.leibnizhu.tinylsm.TestUtils.{checkIterator, entry, tempDir}
import org.scalatest.funsuite.AnyFunSuite

import java.io.File

class LsmStorageTest extends AnyFunSuite {

  test("week1_day1_task2_storage_integration") {
    val options = LsmStorageOptions(4096, 2 << 20, 50, NoCompaction(), false, false)
    val storage = LsmStorageInner(tempDir(), options)

    assert(storage.get("0".getBytes).isEmpty)

    storage.put("1".getBytes, "233".getBytes)
    storage.put("2".getBytes, "2333".getBytes)
    storage.put("3".getBytes, "23333".getBytes)
    assertResult("233".getBytes)(storage.get("1".getBytes).get)
    assertResult("2333".getBytes)(storage.get("2".getBytes).get)
    assertResult("23333".getBytes)(storage.get("3".getBytes).get)

    storage.delete("2".getBytes)
    assert(storage.get("2".getBytes).isEmpty);
    // should NOT report any error
    storage.delete("0".getBytes)
  }

  test("week1_day1_task3_storage_integration") {
    val options = LsmStorageOptions(4096, 2 << 20, 50, NoCompaction(), false, false)
    val storage = LsmStorageInner(tempDir(), options)

    storage.put("1".getBytes, "233".getBytes)
    storage.put("2".getBytes, "2333".getBytes)
    storage.put("3".getBytes, "23333".getBytes)
    storage.forceFreezeMemTable()
    assertResult(1)(storage.state.read(_.immutableMemTables.length))
    val previousApproximateSize = storage.state.read(_.immutableMemTables.head.approximateSize.get)
    assert(previousApproximateSize >= 15)

    storage.put("1".getBytes, "2333".getBytes)
    storage.put("2".getBytes, "23333".getBytes)
    storage.put("3".getBytes, "233333".getBytes)
    storage.forceFreezeMemTable()
    assertResult(2)(storage.state.read(_.immutableMemTables.length))
    assertResult(previousApproximateSize)(storage.state.read(_.immutableMemTables(1).approximateSize.get))
    assert(storage.state.read(_.immutableMemTables.head.approximateSize.get) > previousApproximateSize)
  }

  test("week1_day1_task3_freeze_on_capacity") {
    val options = LsmStorageOptions(4096, 1024, 1000, NoCompaction(), false, false)
    val storage = LsmStorageInner(tempDir(), options)

    for (i <- 0 until 1000) {
      storage.put("1".getBytes, "2333".getBytes)
    }
    val numImmMemTables = storage.state.read(_.immutableMemTables.length)
    assert(numImmMemTables >= 1)

    for (i <- 0 until 1000) {
      storage.delete("1".getBytes)
    }
    assert(storage.state.read(_.immutableMemTables.length) > numImmMemTables)
  }

  test("week1_day1_task4_storage_integration") {
    val options = LsmStorageOptions(4096, 1024, 1000, NoCompaction(), false, false)
    val storage = LsmStorageInner(tempDir(), options)

    assert(storage.get("0".getBytes).isEmpty)

    storage.put("1".getBytes, "233".getBytes)
    storage.put("2".getBytes, "2333".getBytes)
    storage.put("3".getBytes, "23333".getBytes)
    storage.forceFreezeMemTable()
    storage.delete("1".getBytes)
    storage.delete("2".getBytes)
    storage.put("3".getBytes, "2333".getBytes)
    storage.put("4".getBytes, "23333".getBytes)
    storage.forceFreezeMemTable()
    storage.put("1".getBytes, "233333".getBytes)
    storage.put("3".getBytes, "233333".getBytes)
    assertResult(2)(storage.state.read(_.immutableMemTables.length))
    assertResult("233333".getBytes)(storage.get("1".getBytes).get)
    assert(storage.get("2".getBytes).isEmpty)
    assertResult("233333".getBytes)(storage.get("3".getBytes).get)
    assertResult("23333".getBytes)(storage.get("4".getBytes).get)
  }

  test("week1_day2_task4_integration") {
    val options = LsmStorageOptions(4096, 2 << 20, 50, NoCompaction(), false, false)
    val storage = LsmStorageInner(tempDir(), options)

    storage.put("1".getBytes, "233".getBytes)
    storage.put("2".getBytes, "2333".getBytes)
    storage.put("3".getBytes, "23333".getBytes)
    storage.forceFreezeMemTable()
    storage.delete("1".getBytes)
    storage.delete("2".getBytes)
    storage.put("3".getBytes, "2333".getBytes)
    storage.put("4".getBytes, "23333".getBytes)
    storage.forceFreezeMemTable()
    storage.put("1".getBytes, "233333".getBytes)
    storage.put("3".getBytes, "233333".getBytes)

    {
      val iter = storage.scan(Unbounded(), Unbounded())
      checkIterator(List(
        entry("1", "233333"),
        entry("3", "233333"),
        entry("4", "23333")), iter)
      assert(!iter.isValid)
      iter.next()
      iter.next()
      iter.next()
      assert(!iter.isValid)
    }

    {
      val iter = storage.scan(Included("2".getBytes), Included("3".getBytes))
      checkIterator(List(entry("3", "233333")), iter)
      assert(!iter.isValid)
      iter.next()
      iter.next()
      iter.next()
      assert(!iter.isValid)
    }
  }
}
