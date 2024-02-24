package io.github.leibnizhu.tinylsm

import io.github.leibnizhu.tinylsm.TestUtils.{checkIterator, entry, generateSst, tempDir}
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

  test("week1_day5_task2_storage_scan") {
    val options = LsmStorageOptions(4096, 2 << 20, 50, NoCompaction(), false, false)
    val storage = LsmStorageInner(tempDir(), options)
    storage.put("1".getBytes, "233".getBytes)
    storage.put("2".getBytes, "2333".getBytes)
    storage.put("00".getBytes, "2333".getBytes)
    storage.forceFreezeMemTable()
    storage.put("3".getBytes, "23333".getBytes)
    storage.delete("1".getBytes)

    val sst1 = generateSst(10, new File(tempDir(), "10.sst"), List(
      entry("0", "2333333"),
      entry("00", "2333333"),
      entry("4", "23")
    ), Some(storage.blockCache))
    val sst2 = generateSst(11, new File(tempDir(), "11.sst"), List(
      entry("4", "")
    ), Some(storage.blockCache))

    storage.state.write(st => {
      st.l0SsTables = sst2.sstId() :: sst1.sstId() :: st.l0SsTables
      st.ssTables(sst1.sstId()) = sst1
      st.ssTables(sst2.sstId()) = sst2
    })

    /**
     * MemTable
     * 1 -> "", 3 -> 23333
     * 00 -> 23333, 1 -> 233, 2 -> 2333
     * SST
     * 4 -> ""
     * 0 -> 2333333, 00 -> 2333333, 4 -> 23
     */
    printStorage(storage)
    checkIterator(List(
      entry("0", "2333333"),
      entry("00", "2333"),
      entry("2", "2333"),
      entry("3", "23333"),
    ), storage.scan(Unbounded(), Unbounded()))
    checkIterator(List(
      entry("2", "2333")
    ), storage.scan(Included("1".getBytes), Included("2".getBytes)))
    checkIterator(List(
      entry("2", "2333")
    ), storage.scan(Excluded("1".getBytes), Excluded("3".getBytes)))
  }

  private def printStorage(storage: LsmStorageInner): Unit = {
    val itr = storage.scan(Unbounded(), Unbounded())
    print("Storage content: ")
    while (itr.isValid) {
      print(s"${new String(itr.key())} => ${new String(itr.value())}, ")
      itr.next()
    }
    println()
  }
}
