package io.github.leibnizhu.tinylsm.mvcc

import io.github.leibnizhu.tinylsm.TestUtils.{checkIterator, compactionOption, entry, tempDir}
import io.github.leibnizhu.tinylsm.TinyLsm
import io.github.leibnizhu.tinylsm.compact.CompactionOptions
import io.github.leibnizhu.tinylsm.utils.Unbounded
import org.scalatest.funsuite.AnyFunSuite

class LsmMvccTest extends AnyFunSuite {

  test("week3_day3_task2_memtable_mvcc") {
    val options = compactionOption(CompactionOptions.NoCompaction, true)
    val storage = TinyLsm(tempDir(), options)
    storage.put("a", "1")
    storage.put("b", "1")
    val snapshot1 = storage.newTxn()
    storage.put("a", "2")
    val snapshot2 = storage.newTxn()
    storage.delete("b")
    storage.put("c", "1")
    val snapshot3 = storage.newTxn()

    // 验证3个阶段的snapshot
    assertResult("1")(snapshot1.get("a").get)
    assertResult("1")(snapshot1.get("b").get)
    assert(snapshot1.get("c").isEmpty)
    checkIterator(List(
      entry("a", "1"),
      entry("b", "1"),
    ), snapshot1.scan(Unbounded(), Unbounded()))

    assertResult("2")(snapshot2.get("a").get)
    assertResult("1")(snapshot2.get("b").get)
    assert(snapshot2.get("c").isEmpty)
    checkIterator(List(
      entry("a", "2"),
      entry("b", "1"),
    ), snapshot2.scan(Unbounded(), Unbounded()))

    assertResult("2")(snapshot3.get("a").get)
    assert(snapshot3.get("b").isEmpty)
    assertResult("1")(snapshot3.get("c").get)
    checkIterator(List(
      entry("a", "2"),
      entry("c", "1"),
    ), snapshot3.scan(Unbounded(), Unbounded()))

    // froze 一个 memtable 并插入新值，记录snapshot
    storage.inner.forceFreezeMemTable()
    storage.put("a", "3")
    storage.put("b", "3")
    val snapshot4 = storage.newTxn()
    storage.put("a", "4")
    val snapshot5 = storage.newTxn()
    storage.delete("b")
    storage.put("c", "5")
    val snapshot6 = storage.newTxn()

    // snapshot 1-3 不受影响
    assertResult("1")(snapshot1.get("a").get)
    assertResult("1")(snapshot1.get("b").get)
    assert(snapshot1.get("c").isEmpty)
    checkIterator(List(
      entry("a", "1"),
      entry("b", "1"),
    ), snapshot1.scan(Unbounded(), Unbounded()))

    assertResult("2")(snapshot2.get("a").get)
    assertResult("1")(snapshot2.get("b").get)
    assert(snapshot2.get("c").isEmpty)
    checkIterator(List(
      entry("a", "2"),
      entry("b", "1"),
    ), snapshot2.scan(Unbounded(), Unbounded()))

    assertResult("2")(snapshot3.get("a").get)
    assert(snapshot3.get("b").isEmpty)
    assertResult("1")(snapshot3.get("c").get)
    checkIterator(List(
      entry("a", "2"),
      entry("c", "1"),
    ), snapshot3.scan(Unbounded(), Unbounded()))

    // 验证新的3个阶段的snapshot
    assertResult("3")(snapshot4.get("a").get)
    assertResult("3")(snapshot4.get("b").get)
    assertResult("1")(snapshot4.get("c").get)
    checkIterator(List(
      entry("a", "3"),
      entry("b", "3"),
      entry("c", "1"),
    ), snapshot4.scan(Unbounded(), Unbounded()))

    assertResult("4")(snapshot5.get("a").get)
    assertResult("3")(snapshot5.get("b").get)
    assertResult("1")(snapshot5.get("c").get)
    checkIterator(List(
      entry("a", "4"),
      entry("b", "3"),
      entry("c", "1"),
    ), snapshot5.scan(Unbounded(), Unbounded()))

    assertResult("4")(snapshot6.get("a").get)
    assert(snapshot6.get("b").isEmpty)
    assertResult("5")(snapshot6.get("c").get)
    checkIterator(List(
      entry("a", "4"),
      entry("c", "5"),
    ), snapshot6.scan(Unbounded(), Unbounded()))
  }
}
