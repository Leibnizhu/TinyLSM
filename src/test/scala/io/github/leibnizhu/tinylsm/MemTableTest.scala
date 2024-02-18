package io.github.leibnizhu.tinylsm

import org.scalatest.funsuite.AnyFunSuite

import java.io.File

class MemTableTest extends AnyFunSuite {

  test("test_task1_memtable_get") {
    val memTable = MemTable(0)
    memTable.put("key1".getBytes, "value1".getBytes)
    memTable.put("key2".getBytes, "value2".getBytes)
    memTable.put("key3".getBytes, "value3".getBytes)
    assert(memTable.get("key1".getBytes).get sameElements "value1".getBytes)
    assert(memTable.get("key2".getBytes).get sameElements "value2".getBytes)
    assert(memTable.get("key3".getBytes).get sameElements "value3".getBytes)
  }

  test("test_task1_memtable_overwrite") {
    val memTable = MemTable(0)
    memTable.put("key1".getBytes, "value1".getBytes)
    memTable.put("key2".getBytes, "value2".getBytes)
    memTable.put("key3".getBytes, "value3".getBytes)
    memTable.put("key1".getBytes, "value11".getBytes)
    memTable.put("key2".getBytes, "value22".getBytes)
    memTable.put("key3".getBytes, "value33".getBytes)
    assert(memTable.get("key1".getBytes).get sameElements "value11".getBytes)
    assert(memTable.get("key2".getBytes).get sameElements "value22".getBytes)
    assert(memTable.get("key3".getBytes).get sameElements "value33".getBytes)
  }

  test("test_task2_storage_integration") {
    val tempDir = System.getProperty("java.io.tmpdir") + File.pathSeparator + "MemTableTest"
    val options = LsmStorageOptions(4096, 2 << 20, 50, NoCompaction(), false, false)
    val storage = LsmStorageInner(new File(tempDir), options)

    assert(storage.get("0".getBytes).isEmpty)

    storage.put("1".getBytes, "233".getBytes)
    storage.put("2".getBytes, "2333".getBytes)
    storage.put("3".getBytes, "23333".getBytes)
    assert(storage.get("1".getBytes).get sameElements "233".getBytes)
    assert(storage.get("2".getBytes).get sameElements "2333".getBytes)
    assert(storage.get("3".getBytes).get sameElements "23333".getBytes)

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
    assert(storage.state.read(_.immutableMemTables.length) == 1)
    val previousApproximateSize = storage.state.read(_.immutableMemTables.head.approximateSize.get)
    assert(previousApproximateSize >= 15)

    storage.put("1".getBytes, "2333".getBytes)
    storage.put("2".getBytes, "23333".getBytes)
    storage.put("3".getBytes, "233333".getBytes)
    storage.forceFreezeMemTable()
    assert(storage.state.read(_.immutableMemTables.length) == 2)
    assert(storage.state.read(_.immutableMemTables(1).approximateSize.get) == previousApproximateSize)
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
    assert(storage.state.read(_.immutableMemTables.length) == 2)
    assert(storage.get("1".getBytes).get sameElements "233333".getBytes)
    assert(storage.get("2".getBytes).isEmpty)
    assert(storage.get("3".getBytes).get sameElements "233333".getBytes)
    assert(storage.get("4".getBytes).get sameElements "23333".getBytes)
  }
}
