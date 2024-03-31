package io.github.leibnizhu.tinylsm.mvcc

import io.github.leibnizhu.tinylsm.TestUtils.{checkIterator, compactionOption, entry, tempDir}
import io.github.leibnizhu.tinylsm.TinyLsm
import io.github.leibnizhu.tinylsm.compact.CompactionOptions.NoCompaction
import io.github.leibnizhu.tinylsm.utils.Unbounded
import org.scalatest.funsuite.AnyFunSuite

class TransactionTest extends AnyFunSuite {

  test("week3_day5_txn_integration") {
    val dir = tempDir()
    val options = compactionOption(NoCompaction).copy(enableWal = true)
    val storage = TinyLsm(dir, options)
    val txn1 = storage.newTxn()
    val txn2 = storage.newTxn()
    txn1.put("test1", "233")
    txn2.put("test2", "233")
    // transaction 里面能看到自己的变更
    checkIterator(List(
      entry("test1", "233"),
    ), txn1.scan(Unbounded(), Unbounded()))
    checkIterator(List(
      entry("test2", "233"),
    ), txn2.scan(Unbounded(), Unbounded()))

    val txn3 = storage.newTxn()
    // transaction 没提交，不影响新的 transaction 的视图
    checkIterator(List(), txn3.scan(Unbounded(), Unbounded()))

    txn1.commit()
    txn2.commit()
    // 旧transaction 提交不影响已经创建的 transaction 的视图
    checkIterator(List(), txn3.scan(Unbounded(), Unbounded()))

    txn3.rollback()
    // storage 能看到已提交的数据
    checkIterator(List(
      entry("test1", "233"),
      entry("test2", "233"),
    ), storage.scan(Unbounded(), Unbounded()))

    val txn4 = storage.newTxn()
    assertResult("233")(txn4.get("test1").get)
    assertResult("233")(txn4.get("test2").get)
    checkIterator(List(
      entry("test1", "233"),
      entry("test2", "233"),
    ), txn4.scan(Unbounded(), Unbounded()))

    txn4.put("test2", "2333")
    // 视图内能看到自己提交
    checkIterator(List(
      entry("test1", "233"),
      entry("test2", "2333"),
    ), txn4.scan(Unbounded(), Unbounded()))

    txn4.delete("test2")
    assertResult("233")(txn4.get("test1").get)
    assert(txn4.get("test2").isEmpty)
    checkIterator(List(
      entry("test1", "233"),
    ), txn4.scan(Unbounded(), Unbounded()))
  }

  test("week3_day6_serializable_1") {
    val dir = tempDir()
    val options = compactionOption(NoCompaction).copy(enableWal = true).copy(serializable = true)
    val storage = TinyLsm(dir, options)
    storage.put("key1", "1")
    storage.put("key2", "2")
    val txn1 = storage.newTxn()
    val txn2 = storage.newTxn()
    txn1.put("key1", txn1.get("key2").get)
    txn2.put("key2", txn2.get("key1").get)
    txn1.commit()
    assertThrows[Exception](txn2.commit())
    txn2.rollback()
    assertResult(Some("2"))(storage.get("key1"))
    assertResult(Some("2"))(storage.get("key2"))
  }

  test("week3_day6_serializable_2") {
    val dir = tempDir()
    val options = compactionOption(NoCompaction).copy(enableWal = true).copy(serializable = true)
    val storage = TinyLsm(dir, options)
    val txn1 = storage.newTxn()
    val txn2 = storage.newTxn()
    txn1.put("key1", "1")
    txn2.put("key1", "2")
    txn1.commit()
    txn2.commit()
    assertResult(Some("2"))(storage.get("key1"))
  }

  test("week3_day6_serializable_3_ts_range") {
    val dir = tempDir()
    val options = compactionOption(NoCompaction).copy(enableWal = true).copy(serializable = true)
    val storage = TinyLsm(dir, options)
    storage.put("key1", "1")
    storage.put("key2", "2")
    val txn1 = storage.newTxn()
    txn1.put("key1", txn1.get("key2").get)
    txn1.commit()
    val txn2 = storage.newTxn()
    txn2.put("key2", txn2.get("key1").get)
    txn2.commit()
    txn2.rollback()
    assertResult("2")(storage.get("key1").get)
    assertResult("2")(storage.get("key2").get)
  }

  test("week3_day6_serializable_4_scan") {
    val dir = tempDir()
    val options = compactionOption(NoCompaction).copy(enableWal = true).copy(serializable = true)
    val storage = TinyLsm(dir, options)
    storage.put("key1", "1")
    storage.put("key2", "2")
    val txn1 = storage.newTxn()
    val txn2 = storage.newTxn()
    txn1.put("key1", txn1.get("key2").get)
    txn1.commit()
    val iter = txn2.scan(Unbounded(), Unbounded())
    while (iter.isValid) {
      iter.next()
    }
    txn2.put("key2", "1")
    assertThrows[Exception](txn2.commit())
    txn2.rollback()
    assertResult(Some("2"))(storage.get("key1"))
    assertResult(Some("2"))(storage.get("key2"))
  }

  test("week3_day6_serializable_5_read_only") {
    val dir = tempDir()
    val options = compactionOption(NoCompaction).copy(enableWal = true).copy(serializable = true)
    val storage = TinyLsm(dir, options)
    storage.put("key1", "1")
    storage.put("key2", "2")
    val txn1 = storage.newTxn()
    txn1.put("key1", txn1.get("key2").get)
    txn1.commit()
    val txn2 = storage.newTxn()
    txn2.get("key1")
    val iter = txn2.scan(Unbounded(), Unbounded())
    while (iter.isValid) {
      iter.next()
    }
    txn2.commit()
    assertResult(Some("2"))(storage.get("key1"))
    assertResult(Some("2"))(storage.get("key2"))
  }
}
