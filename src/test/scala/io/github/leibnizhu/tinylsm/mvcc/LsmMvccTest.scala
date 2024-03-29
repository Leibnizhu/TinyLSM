package io.github.leibnizhu.tinylsm.mvcc

import io.github.leibnizhu.tinylsm.TestUtils.{checkIterator, compactionOption, entry, tempDir}
import io.github.leibnizhu.tinylsm.compact.CompactionOptions
import io.github.leibnizhu.tinylsm.utils.{Excluded, Included, Unbounded}
import io.github.leibnizhu.tinylsm.{MemTableKey, SsTableBuilder, TestUtils, TinyLsm}
import org.scalatest.funsuite.AnyFunSuite

import java.io.File

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

  test("week3_day3_task2_lsm_iterator_mvcc") {
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
    storage.inner.forceFreezeMemTable()
    storage.inner.forceFlushNextImmutableMemTable()

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
    assert(snapshot3.get("b").isEmpty);
    assertResult("1")(snapshot3.get("c").get)
    checkIterator(List(
      entry("a", "2"),
      entry("c", "1"),
    ), snapshot3.scan(Unbounded(), Unbounded()))

    storage.put("a", "3")
    storage.put("b", "3")
    val snapshot4 = storage.newTxn()
    storage.put("a", "4")
    val snapshot5 = storage.newTxn()
    storage.delete("b")
    storage.put("c", "5")
    val snapshot6 = storage.newTxn()
    storage.inner.forceFreezeMemTable()
    storage.inner.forceFlushNextImmutableMemTable()

    assertResult("1")(snapshot1.get("a").get)
    assertResult("1")(snapshot1.get("b").get)
    assert(snapshot1.get("c").isEmpty);
    checkIterator(List(
      entry("a", "1"),
      entry("b", "1"),
    ), snapshot1.scan(Unbounded(), Unbounded()))

    assertResult("2")(snapshot2.get("a").get)
    assertResult("1")(snapshot2.get("b").get)
    assert(snapshot2.get("c").isEmpty);
    checkIterator(List(
      entry("a", "2"),
      entry("b", "1"),
    ), snapshot2.scan(Unbounded(), Unbounded()))

    assertResult("2")(snapshot3.get("a").get)
    assert(snapshot3.get("b").isEmpty);
    assertResult("1")(snapshot3.get("c").get)
    checkIterator(List(
      entry("a", "2"),
      entry("c", "1"),
    ), snapshot3.scan(Unbounded(), Unbounded()))

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
    assert(snapshot6.get("b").isEmpty);
    assertResult("5")(snapshot6.get("c").get)
    checkIterator(List(
      entry("a", "4"),
      entry("c", "5"),
    ), snapshot6.scan(Unbounded(), Unbounded()))

    checkIterator(List(
      entry("a", "4"),
    ), snapshot6.scan(Included("a"), Included("a")))

    TestUtils.dumpIterator(snapshot6.scan(Excluded("a"), Excluded("c")))
    checkIterator(List(), snapshot6.scan(Excluded("a"), Excluded("b")))
    checkIterator(List(), snapshot6.scan(Excluded("a"), Excluded("c")))
  }

  test("week3_day3_task3_sst_ts") {
    val builder = new SsTableBuilder(16);
    builder.add(MemTableKey("11".getBytes, 1), "11".getBytes)
    builder.add(MemTableKey("22".getBytes, 2), "22".getBytes)
    builder.add(MemTableKey("33".getBytes, 3), "11".getBytes)
    builder.add(MemTableKey("44".getBytes, 4), "22".getBytes)
    builder.add(MemTableKey("55".getBytes, 5), "11".getBytes)
    builder.add(MemTableKey("66".getBytes, 6), "22".getBytes)
    val sst = builder.build(0, None, new File(tempDir(), "1.sst"))
    assertResult(6)(sst.maxTimestamp);
  }
}
