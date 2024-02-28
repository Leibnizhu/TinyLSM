package io.github.leibnizhu.tinylsm

import io.github.leibnizhu.tinylsm.utils.{Excluded, Included, Unbounded}
import org.scalatest.funsuite.AnyFunSuite

class MemTableTest extends AnyFunSuite {

  test("week1_day1_task1_memtable_get") {
    val memTable = MemTable(0)
    memTable.put("key1".getBytes, "value1".getBytes)
    memTable.put("key2".getBytes, "value2".getBytes)
    memTable.put("key3".getBytes, "value3".getBytes)
    assertResult("value1".getBytes)(memTable.get("key1".getBytes).get)
    assertResult("value2".getBytes)(memTable.get("key2".getBytes).get)
    assertResult("value3".getBytes)(memTable.get("key3".getBytes).get)
  }

  test("week1_day1_task1_memtable_overwrite") {
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

  test("week1_day2_task1_memtable_iter_empty") {
    val memTable = MemTable(0)
    val iter = memTable.scan(Unbounded(), Unbounded())
    assert(!iter.isValid)
  }

  test("week1_day2_task1_memtable_iter") {
    val memTable = MemTable(0)
    memTable.put("key2".getBytes, "value2".getBytes)
    memTable.put("key1".getBytes, "value1".getBytes)
    memTable.put("key3".getBytes, "value3".getBytes)

    {
      val iter = memTable.scan(Unbounded(), Unbounded())
      assert(iter.isValid)
      assertResult("key1".getBytes)(iter.key())
      assertResult("value1".getBytes)(iter.value())
      iter.next()
      assert(iter.isValid)
      assertResult("key2".getBytes)(iter.key())
      assertResult("value2".getBytes)(iter.value())
      iter.next()
      assert(iter.isValid)
      assertResult("key3".getBytes)(iter.key())
      assertResult("value3".getBytes)(iter.value())
      iter.next()
      assert(!iter.isValid)
    }

    {
      val iter = memTable.scan(Included("key1"), Included("key2"))
      assert(iter.isValid)
      assertResult("key1".getBytes)(iter.key())
      assertResult("value1".getBytes)(iter.value())
      iter.next()
      assert(iter.isValid)
      assertResult("key2".getBytes)(iter.key())
      assertResult("value2".getBytes)(iter.value())
      iter.next()
      assert(!iter.isValid)
    }

    {
      val iter = memTable.scan(Excluded("key1"), Excluded("key3"))
      assert(iter.isValid)
      assertResult("key2".getBytes)(iter.key())
      assertResult("value2".getBytes)(iter.value())
      iter.next()
      assert(!iter.isValid)
    }
  }

  test("week1_day2_task1_empty_memtable_iter") {
    val memTable = MemTable(0)
    {
      val iter = memTable.scan(Excluded("key1"), Excluded("key3"))
      assert(!iter.isValid)
    }
    {
      val iter = memTable.scan(Included("key1"), Included("key2"))
      assert(!iter.isValid)
    }
    {
      val iter = memTable.scan(Unbounded(), Unbounded())
      assert(!iter.isValid)
    }
  }
}
