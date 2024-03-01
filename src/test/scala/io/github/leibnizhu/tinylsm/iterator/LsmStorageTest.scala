package io.github.leibnizhu.tinylsm.iterator

import io.github.leibnizhu.tinylsm.*
import io.github.leibnizhu.tinylsm.TestUtils.*
import io.github.leibnizhu.tinylsm.compact.CompactionOptions
import io.github.leibnizhu.tinylsm.iterator.*
import io.github.leibnizhu.tinylsm.utils.{Excluded, Included, Unbounded}
import org.scalatest.funsuite.AnyFunSuite

import java.io.File

class LsmStorageTest extends AnyFunSuite {

  test("week1_day1_task2_storage_integration") {
    val options = LsmStorageOptions.defaultOption()
    val storage = LsmStorageInner(tempDir(), options)

    assert(storage.get("0").isEmpty)

    storage.put("1", "233")
    storage.put("2", "2333")
    storage.put("3", "23333")
    assertResult("233")(storage.get("1").get)
    assertResult("2333")(storage.get("2").get)
    assertResult("23333")(storage.get("3").get)

    storage.delete("2")
    assert(storage.get("2").isEmpty);
    // should NOT report any error
    storage.delete("0")
  }

  test("week1_day1_task3_storage_integration") {
    val options = LsmStorageOptions.defaultOption()
    val storage = LsmStorageInner(tempDir(), options)

    storage.put("1", "233")
    storage.put("2", "2333")
    storage.put("3", "23333")
    storage.forceFreezeMemTable()
    assertResult(1)(storage.state.read(_.immutableMemTables.length))
    val previousApproximateSize = storage.state.read(_.immutableMemTables.head.approximateSize.get)
    assert(previousApproximateSize >= 15)

    storage.put("1", "2333")
    storage.put("2", "23333")
    storage.put("3", "233333")
    storage.forceFreezeMemTable()
    assertResult(2)(storage.state.read(_.immutableMemTables.length))
    assertResult(previousApproximateSize)(storage.state.read(_.immutableMemTables(1).approximateSize.get))
    assert(storage.state.read(_.immutableMemTables.head.approximateSize.get) > previousApproximateSize)
  }

  test("week1_day1_task3_freeze_on_capacity") {
    val options = LsmStorageOptions(4096, 1024, 1000, CompactionOptions.NoCompaction, false, false)
    val storage = LsmStorageInner(tempDir(), options)

    for (i <- 0 until 1000) {
      storage.put("1", "2333")
    }
    val numImmMemTables = storage.state.read(_.immutableMemTables.length)
    assert(numImmMemTables >= 1)

    for (i <- 0 until 1000) {
      storage.delete("1")
    }
    assert(storage.state.read(_.immutableMemTables.length) > numImmMemTables)
  }

  test("week1_day1_task4_storage_integration") {
    val options = LsmStorageOptions(4096, 1024, 1000, CompactionOptions.NoCompaction, false, false)
    val storage = LsmStorageInner(tempDir(), options)

    assert(storage.get("0").isEmpty)

    storage.put("1", "233")
    storage.put("2", "2333")
    storage.put("3", "23333")
    storage.forceFreezeMemTable()
    storage.delete("1")
    storage.delete("2")
    storage.put("3", "2333")
    storage.put("4", "23333")
    storage.forceFreezeMemTable()
    storage.put("1", "233333")
    storage.put("3", "233333")
    assertResult(2)(storage.state.read(_.immutableMemTables.length))
    assertResult("233333")(storage.get("1").get)
    assert(storage.get("2").isEmpty)
    assertResult("233333")(storage.get("3").get)
    assertResult("23333")(storage.get("4").get)
  }

  test("week1_day2_task4_integration") {
    val options = LsmStorageOptions.defaultOption()
    val storage = LsmStorageInner(tempDir(), options)

    storage.put("1", "233")
    storage.put("2", "2333")
    storage.put("3", "23333")
    storage.forceFreezeMemTable()
    storage.delete("1")
    storage.delete("2")
    storage.put("3", "2333")
    storage.put("4", "23333")
    storage.forceFreezeMemTable()
    storage.put("1", "233333")
    storage.put("3", "233333")

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
      val iter = storage.scan(Included("2"), Included("3"))
      checkIterator(List(entry("3", "233333")), iter)
      assert(!iter.isValid)
      iter.next()
      iter.next()
      iter.next()
      assert(!iter.isValid)
    }
  }

  test("week1_day5_task2_storage_scan") {
    val options = LsmStorageOptions.defaultOption()
    val storage = LsmStorageInner(tempDir(), options)
    storage.put("1", "233")
    storage.put("2", "2333")
    storage.put("00", "2333")
    storage.forceFreezeMemTable()
    storage.put("3", "23333")
    storage.delete("1")

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
      st.ssTables = st.ssTables + (sst1.sstId() -> sst1)
      st.ssTables = st.ssTables + (sst2.sstId() -> sst2)
    })

    /**
     * MemTable
     * 1 -> "", 3 -> 23333
     * 00 -> 23333, 1 -> 233, 2 -> 2333
     * SST
     * 4 -> ""
     * 0 -> 2333333, 00 -> 2333333, 4 -> 23
     */
    storage.dumpStorage()
    checkIterator(List(
      entry("0", "2333333"),
      entry("00", "2333"),
      entry("2", "2333"),
      entry("3", "23333"),
    ), storage.scan(Unbounded(), Unbounded()))

    checkIterator(List(
      entry("2", "2333")
    ), storage.scan(Included("1"), Included("2")))

    checkIterator(List(
      entry("2", "2333")
    ), storage.scan(Excluded("1"), Excluded("3")))
  }

  test("week1_day5_task3_storage_get") {
    val options = LsmStorageOptions.defaultOption()
    val storage = LsmStorageInner(tempDir(), options)
    storage.put("1", "233")
    storage.put("2", "2333")
    storage.put("00", "2333")
    storage.forceFreezeMemTable()
    storage.put("3", "23333")
    storage.delete("1")

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
      st.ssTables = st.ssTables + (sst1.sstId() -> sst1)
      st.ssTables = st.ssTables + (sst2.sstId() -> sst2)
    })

    assertResult("2333333")(storage.get("0").get)
    assertResult("2333")(storage.get("00").get)
    assertResult("2333")(storage.get("2").get)
    assertResult("23333")(storage.get("3").get)
    assert(storage.get("4").isEmpty)
    assert(storage.get("--").isEmpty)
    assert(storage.get("5").isEmpty)
  }

  test("week1_day6_task1_storage_scan") {
    val options = LsmStorageOptions.defaultOption()
    val storage = LsmStorageInner(tempDir(), options)
    storage.put("0", "2333333")
    storage.put("00", "2333333")
    storage.put("4", "23")
    syncStorage(storage)
    storage.delete("4")
    syncStorage(storage)
    storage.put("1", "233")
    storage.put("2", "2333")
    storage.forceFreezeMemTable()
    storage.put("00", "2333")
    storage.forceFreezeMemTable()
    storage.put("3", "23333")
    storage.delete("1")

    storage.state.read(st => {
      assertResult(2)(st.l0SsTables.length)
      assertResult(2)(st.immutableMemTables.length)
    })

    checkIterator(List(
      entry("0", "2333333"),
      entry("00", "2333"),
      entry("2", "2333"),
      entry("3", "23333"),
    ), storage.scan(Unbounded(), Unbounded()))

    checkIterator(List(
      entry("2", "2333"),
    ), storage.scan(Included("1"), Included("2")))

    checkIterator(List(
      entry("2", "2333"),
    ), storage.scan(Excluded("1"), Excluded("3")))
  }

  test("week1_day6_task1_storage_get") {
    val options = LsmStorageOptions.defaultOption()
    val storage = LsmStorageInner(tempDir(), options)
    storage.put("0", "2333333")
    storage.put("00", "2333333")
    storage.put("4", "23")
    syncStorage(storage)
    storage.delete("4")
    syncStorage(storage)
    storage.put("1", "233")
    storage.put("2", "2333")
    storage.forceFreezeMemTable()
    storage.put("00", "2333")
    storage.forceFreezeMemTable()
    storage.put("3", "23333")
    storage.delete("1")

    storage.state.read(st => {
      assertResult(2)(st.l0SsTables.length)
      assertResult(2)(st.immutableMemTables.length)
    })

    assertResult("2333333")(storage.get("0").get)
    assertResult("2333")(storage.get("00").get)
    assertResult("2333")(storage.get("2").get)
    assertResult("23333")(storage.get("3").get)
    assert(storage.get("4").isEmpty)
    assert(storage.get("--").isEmpty)
    assert(storage.get("555").isEmpty)
  }

  test("week1_day6_task2_auto_flush") {
    val options = LsmStorageOptions(4096, 2 << 20, 2, CompactionOptions.NoCompaction, false, false)
    val storage = TinyLsm(tempDir(), options)
    val value = "1" * 1024
    for (i <- 0 until 6000) {
      storage.put(i.toString, value)
    }
    Thread.sleep(1000)
    storage.inner.state.read(st => assert(st.l0SsTables.nonEmpty))
    storage.close()
  }

  test("week1_day6_task3_sst_filter") {
    val options = LsmStorageOptions.defaultOption()
    val storage = LsmStorageInner(tempDir(), options)
    val keyFormat = "%05d"
    for (i <- 1 to 10000) {
      if (i % 1000 == 0) {
        syncStorage(storage)
      }
      storage.put(keyFormat.format(i), "2333333")
    }
    val iter1 = storage.scan(Unbounded(), Unbounded())
    val maxIterNum = iter1.numActiveIterators()
    assert(maxIterNum >= 10, s"current active iterators: $maxIterNum")

    val iter2 = storage.scan(Excluded(keyFormat.format(10000)), Unbounded())
    val minIterNum = iter2.numActiveIterators()
    assert(minIterNum < maxIterNum)

    val iter3 = storage.scan(Unbounded(), Excluded(keyFormat.format(1)))
    assertResult(minIterNum)(iter3.numActiveIterators())

    val iter4 = storage.scan(Unbounded(), Included(keyFormat.format(0)))
    assertResult(minIterNum)(iter4.numActiveIterators())

    val iter5 = storage.scan(Included(keyFormat.format(10001)), Unbounded())
    assertResult(minIterNum)(iter5.numActiveIterators())

    val iter6 = storage.scan(Included(keyFormat.format(5000)), Excluded(keyFormat.format(6000)))
    assert(minIterNum < iter6.numActiveIterators())
    assert(maxIterNum > iter6.numActiveIterators())
  }
}
