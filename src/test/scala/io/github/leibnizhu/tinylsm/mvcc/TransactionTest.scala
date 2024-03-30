package io.github.leibnizhu.tinylsm.mvcc

import io.github.leibnizhu.tinylsm.TestUtils.{checkIterator, compactionOption, entry, tempDir}
import io.github.leibnizhu.tinylsm.TinyLsm
import io.github.leibnizhu.tinylsm.compact.CompactionOptions.NoCompaction
import io.github.leibnizhu.tinylsm.utils.Unbounded
import org.scalatest.funsuite.AnyFunSuite

class TransactionTest extends AnyFunSuite {

  test("week3_day5_txn_integration") {
    val dir = tempDir()
    val options = compactionOption(NoCompaction, true)
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
}
