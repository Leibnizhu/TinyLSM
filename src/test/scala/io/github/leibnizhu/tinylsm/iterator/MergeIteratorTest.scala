package io.github.leibnizhu.tinylsm.iterator

import io.github.leibnizhu.tinylsm.TestUtils.*
import io.github.leibnizhu.tinylsm.iterator.{MemTableIterator, *}
import io.github.leibnizhu.tinylsm.{MemTableEntry}
import org.mockito.Mockito.{mock, when}
import org.scalatest.Entry
import org.scalatest.funsuite.AnyFunSuite

class MergeIteratorTest extends AnyFunSuite {

  test("week_day2_task2_merge_1") {
    val i1 = List(
      entry("a", "1.1"),
      entry("b", "2.1"),
      entry("c", "3.1"),
      entry("e", "")
    )
    val i2 = List(
      entry("a", "1.2"),
      entry("b", "2.2"),
      entry("c", "3.2"),
      entry("d", "4.2")
    )
    val i3 = List(
      entry("b", "2.3"),
      entry("c", "3.3"),
      entry("d", "4.3")
    )
    val iter = MergeIterator(List(
      MemTableIterator(i1.iterator),
      MemTableIterator(i2.iterator),
      MemTableIterator(i3.iterator)))
    checkIterator(List(
      entry("a", "1.1"),
      entry("b", "2.1"),
      entry("c", "3.1"),
      entry("d", "4.2"),
      entry("e", "")
    ), iter)

    val iter2 = MergeIterator(List(
      MemTableIterator(i3.iterator),
      MemTableIterator(i1.iterator),
      MemTableIterator(i2.iterator)))
    checkIterator(List(
      entry("a", "1.1"),
      entry("b", "2.3"),
      entry("c", "3.3"),
      entry("d", "4.3"),
      entry("e", "")
    ), iter2)
  }

  test("week_day2_task2_merge_2") {
    val i1 = List(
      entry("a", "1.1"),
      entry("b", "2.1"),
      entry("c", "3.1")
    )
    val i2 = List(
      entry("d", "1.2"),
      entry("e", "2.2"),
      entry("f", "3.2"),
      entry("g", "4.2")
    )
    val i3 = List(
      entry("h", "1.3"),
      entry("i", "2.3"),
      entry("j", "3.3"),
      entry("k", "4.3")
    )
    val i4 = List()
    val iter = MergeIterator(List(
      MemTableIterator(i1.iterator),
      MemTableIterator(i2.iterator),
      MemTableIterator(i3.iterator),
      MemTableIterator(i4.iterator)))
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
    )
    checkIterator(expect, iter)

    // key都唯一，没有覆盖过所以顺序不变
    val iter2 = MergeIterator(List(
      MemTableIterator(i2.iterator),
      MemTableIterator(i4.iterator),
      MemTableIterator(i3.iterator),
      MemTableIterator(i1.iterator)))
    checkIterator(expect, iter2)

    // key都唯一，没有覆盖过所以顺序不变
    val iter3 = MergeIterator(List(
      MemTableIterator(i4.iterator),
      MemTableIterator(i3.iterator),
      MemTableIterator(i2.iterator),
      MemTableIterator(i1.iterator)))
    checkIterator(expect, iter3)
  }

  test("week_day2_task2_merge_empty") {
    val iter1 = MergeIterator(List())
    checkIterator(List(), iter1)

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
    ), iter2)
  }

  test("week_day2_task2_merge_error") {
    val iter1 = MergeIterator(List())
    checkIterator(List(), iter1)

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
}
