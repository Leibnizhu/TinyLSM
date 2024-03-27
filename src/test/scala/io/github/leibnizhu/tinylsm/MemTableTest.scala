package io.github.leibnizhu.tinylsm

import io.github.leibnizhu.tinylsm.utils.{Excluded, Included, Unbounded}
import org.scalatest.funsuite.AnyFunSuite

class MemTableTest extends AnyFunSuite {

  test("week1_day1_task1_memtable_get") {
    val memTable = MemTable(0)
    memTable.put(MemTableKey.applyForTest("key1"), "value1".getBytes)
    memTable.put(MemTableKey.applyForTest("key2"), "value2".getBytes)
    memTable.put(MemTableKey.applyForTest("key3"), "value3".getBytes)
    assertResult("value1".getBytes)(memTable.get(MemTableKey.applyForTest("key1")).get)
    assertResult("value2".getBytes)(memTable.get(MemTableKey.applyForTest("key2")).get)
    assertResult("value3".getBytes)(memTable.get(MemTableKey.applyForTest("key3")).get)
  }

  test("week1_day1_task1_memtable_overwrite") {
    val memTable = MemTable(0)
    memTable.put(MemTableKey.applyForTest("key1"), "value1".getBytes)
    memTable.put(MemTableKey.applyForTest("key2"), "value2".getBytes)
    memTable.put(MemTableKey.applyForTest("key3"), "value3".getBytes)
    memTable.put(MemTableKey.applyForTest("key1"), "value11".getBytes)
    memTable.put(MemTableKey.applyForTest("key2"), "value22".getBytes)
    memTable.put(MemTableKey.applyForTest("key3"), "value33".getBytes)
    assertResult("value11".getBytes)(memTable.get(MemTableKey.applyForTest("key1")).get)
    assertResult("value22".getBytes)(memTable.get(MemTableKey.applyForTest("key2")).get)
    assertResult("value33".getBytes)(memTable.get(MemTableKey.applyForTest("key3")).get)
  }

  test("week1_day2_task1_memtable_iter_empty") {
    val memTable = MemTable(0)
    val iter = memTable.scan(Unbounded(), Unbounded())
    assert(!iter.isValid)
  }

  test("week1_day2_task1_memtable_iter") {
    val memTable = MemTable(0)
    memTable.put(MemTableKey.applyForTest("key2"), "value2".getBytes)
    memTable.put(MemTableKey.applyForTest("key1"), "value1".getBytes)
    memTable.put(MemTableKey.applyForTest("key3"), "value3".getBytes)

    {
      val iter = memTable.scan(Unbounded(), Unbounded())
      assert(iter.isValid)
      assertResult("key1".getBytes)(iter.key().bytes)
      assertResult("value1".getBytes)(iter.value())
      iter.next()
      assert(iter.isValid)
      assertResult("key2".getBytes)(iter.key().bytes)
      assertResult("value2".getBytes)(iter.value())
      iter.next()
      assert(iter.isValid)
      assertResult("key3".getBytes)(iter.key().bytes)
      assertResult("value3".getBytes)(iter.value())
      iter.next()
      assert(!iter.isValid)
    }

    {
      val iter = memTable.scan(Included("key1", 0), Included("key2", 0))
      assert(iter.isValid)
      assertResult("key1".getBytes)(iter.key().bytes)
      assertResult("value1".getBytes)(iter.value())
      iter.next()
      assert(iter.isValid)
      assertResult("key2".getBytes)(iter.key().bytes)
      assertResult("value2".getBytes)(iter.value())
      iter.next()
      assert(!iter.isValid)
    }

    {
      val iter = memTable.scan(Excluded("key1", 0), Excluded("key3", 0))
      assert(iter.isValid)
      assertResult("key2".getBytes)(iter.key().bytes)
      assertResult("value2".getBytes)(iter.value())
      iter.next()
      assert(!iter.isValid)
    }
  }

  test("week1_day2_task1_empty_memtable_iter") {
    val memTable = MemTable(0)
    {
      val iter = memTable.scan(Excluded("key1", 0), Excluded("key3", 0))
      assert(!iter.isValid)
    }
    {
      val iter = memTable.scan(Included("key1", 0), Included("key2", 0))
      assert(!iter.isValid)
    }
    {
      val iter = memTable.scan(Unbounded(), Unbounded())
      assert(!iter.isValid)
    }
  }
}
