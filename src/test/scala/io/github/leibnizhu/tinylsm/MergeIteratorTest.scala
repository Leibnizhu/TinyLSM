package io.github.leibnizhu.tinylsm

import org.mockito.Mockito.{mock, when}
import org.scalatest.Entry
import org.scalatest.funsuite.AnyFunSuite

class MergeIteratorTest extends AnyFunSuite {

  test("week_day2_task2_merge_1") {
    val i1 = MemTableIterator(List(
      entry("a", "1.1"),
      entry("b", "2.1"),
      entry("c", "3.1"),
      entry("e", "")
    ).iterator)
    val i2 = MemTableIterator(List(
      entry("a", "1.2"),
      entry("b", "2.2"),
      entry("c", "3.2"),
      entry("d", "4.2")
    ).iterator)
    val i3 = MemTableIterator(List(
      entry("b", "2.3"),
      entry("c", "3.3"),
      entry("d", "4.3")
    ).iterator)
    val iter = MergeIterator(List(i1, i2, i3))
    checkIterator(List(
      entry("a", "1.1"),
      entry("b", "2.1"),
      entry("c", "3.1"),
      entry("d", "4.2"),
      entry("e", "")
    ).iterator, iter)

    val iter2 = MergeIterator(List(i3, i1, i2))
    checkIterator(List(
      entry("a", "1.1"),
      entry("b", "2.3"),
      entry("c", "3.3"),
      entry("d", "4.3"),
      entry("e", "")
    ).iterator, iter2)
  }

  test("week_day2_task2_merge_2") {
    val i1 = MemTableIterator(List(
      entry("a", "1.1"),
      entry("b", "2.1"),
      entry("c", "3.1")
    ).iterator)
    val i2 = MemTableIterator(List(
      entry("d", "1.2"),
      entry("e", "2.2"),
      entry("f", "3.2"),
      entry("g", "4.2")
    ).iterator)
    val i3 = MemTableIterator(List(
      entry("h", "1.3"),
      entry("i", "2.3"),
      entry("j", "3.3"),
      entry("k", "4.3")
    ).iterator)
    val i4 = MemTableIterator(List().iterator)
    val iter = MergeIterator(List(i1, i2, i3, i4))
    val expect = List(
      entry("a", "1.1"),
      entry("b", "2.1"),
      entry("c", "3.1"),
      entry("d", "1.2"),
      entry("e", "2.2"),
      entry("f", "3.2"),
      entry("g", "4.2"),
      entry("h", "1.3"),
      entry("i", "2.3"),
      entry("j", "3.3"),
      entry("k", "4.3"),
    ).iterator
    checkIterator(expect, iter)

    // key都唯一，没有覆盖过所以顺序不变
    val iter2 = MergeIterator(List(i2, i4, i3, i1))
    checkIterator(expect, iter2)

    // key都唯一，没有覆盖过所以顺序不变
    val iter3 = MergeIterator(List(i4, i3, i2, i1))
    checkIterator(expect, iter3)
  }

  test("week_day2_task2_merge_empty") {
    val iter1 = MergeIterator(List())
    checkIterator(List().iterator, iter1)

    val i1 = MemTableIterator(List(
      entry("a", "1.1"),
      entry("b", "2.1"),
      entry("c", "3.1")
    ).iterator)
    val i2 = MemTableIterator(List().iterator)
    val iter2 = MergeIterator(List(i1, i2))
    checkIterator(List(
      entry("a", "1.1"),
      entry("b", "2.1"),
      entry("c", "3.1")
    ).iterator, iter2)
  }

  test("week_day2_task2_merge_error") {
    val iter1 = MergeIterator(List())
    checkIterator(List().iterator, iter1)

    val i1 = MemTableIterator(List(
      entry("a", "1.1"),
      entry("b", "2.1"),
      entry("c", "3.1")
    ).iterator)

    val errIter = mock(classOf[Iterator[MemTableEntry]])
    when(errIter.hasNext).thenReturn(true).thenReturn(true).thenReturn(false)
    when(errIter.next()).thenReturn(entry("a", "1.1")).thenThrow(new RuntimeException())
    val iter = MergeIterator(List(i1,
      MemTableIterator(List.from(i1.iterator.toList).iterator),
      MemTableIterator(errIter)))
    // your implementation should correctly throw an error instead of panic
    expectIteratorError(iter)
  }

  private def entry(k: String, v: String): MemTableEntry = {
    Entry(ByteArrayKey(k.getBytes), v.getBytes)
  }

  private def checkIterator(expect: Iterator[MemTableEntry], actual: MemTableStorageIterator): Unit = {
    while (expect.hasNext) {
      val expectEntry = expect.next()
      assert(actual.isValid)
      actual.next()
      assertResult(expectEntry.getKey.bytes)(actual.key())
      assertResult(expectEntry.getValue)(actual.value())
    }
    assert(!actual.isValid)
  }

  private def expectIteratorError(actual: MemTableStorageIterator): Unit = {
    assertThrows[Exception](() => {
      while (actual.isValid) {
        actual.next()
      }
    })
  }
}
