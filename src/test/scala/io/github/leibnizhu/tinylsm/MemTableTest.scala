package io.github.leibnizhu.tinylsm

import org.scalatest.funsuite.AnyFunSuite

import java.io.File

class MemTableTest extends AnyFunSuite {

  test("test_task1_memtable_get") {
    val memTable = MemTable(0)
    memTable.put("key1".getBytes, "value1".getBytes)
    memTable.put("key2".getBytes, "value2".getBytes)
    memTable.put("key3".getBytes, "value3".getBytes)
    assertResult("value1".getBytes)(memTable.get("key1".getBytes).get)
    assertResult("value2".getBytes)(memTable.get("key2".getBytes).get)
    assertResult("value3".getBytes)(memTable.get("key3".getBytes).get)
  }

  test("test_task1_memtable_overwrite") {
    val memTable = MemTable(0)
    memTable.put("key1".getBytes, "value1".getBytes)
    memTable.put("key2".getBytes, "value2".getBytes)
    memTable.put("key3".getBytes, "value3".getBytes)
    memTable.put("key1".getBytes, "value11".getBytes)
    memTable.put("key2".getBytes, "value22".getBytes)
    memTable.put("key3".getBytes, "value33".getBytes)
    assertResult("value11".getBytes)(memTable.get("key1".getBytes).get)
    assertResult("value22".getBytes)(memTable.get("key2".getBytes).get)
    assertResult("value33".getBytes)(memTable.get("key3".getBytes).get)
  }

  test("test_task2_storage_integration") {
    val tempDir = System.getProperty("java.io.tmpdir") + File.pathSeparator + "MemTableTest"
    val options = LsmStorageOptions(4096, 2 << 20, 50, NoCompaction(), false, false)
    val storage = LsmStorageInner(new File(tempDir), options)

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

  test("test_task3_storage_integration") {
    val tempDir = System.getProperty("java.io.tmpdir") + File.pathSeparator + "MemTableTest"
    val options = LsmStorageOptions(4096, 2 << 20, 50, NoCompaction(), false, false)
    val storage = LsmStorageInner(new File(tempDir), options)

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

  test("test_task3_freeze_on_capacity") {
    val tempDir = System.getProperty("java.io.tmpdir") + File.pathSeparator + "MemTableTest"
    val options = LsmStorageOptions(4096, 1024, 1000, NoCompaction(), false, false)
    val storage = LsmStorageInner(new File(tempDir), options)

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

  test("test_task4_storage_integration") {
    val tempDir = System.getProperty("java.io.tmpdir") + File.pathSeparator + "MemTableTest"
    val options = LsmStorageOptions(4096, 1024, 1000, NoCompaction(), false, false)
    val storage = LsmStorageInner(new File(tempDir), options)

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
}
