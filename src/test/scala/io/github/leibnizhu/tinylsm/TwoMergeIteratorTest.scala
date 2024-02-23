package io.github.leibnizhu.tinylsm

import io.github.leibnizhu.tinylsm.TestUtils.{checkIterator, entry}
import org.scalatest.funsuite.AnyFunSuite

class TwoMergeIteratorTest extends AnyFunSuite {

  test("week1_day5_task1_merge_1") {
    val i1 = MemTableIterator(List(
      entry("a", "1.1"),
      entry("b", "2.1"),
      entry("c", "3.1"),
    ).iterator)
    val i2 = MemTableIterator(List(
      entry("a", "1.2"),
      entry("b", "2.2"),
      entry("c", "3.2"),
      entry("d", "4.2"),
    ).iterator)
    val iter = TwoMergeIterator(i1, i2)
    checkIterator(List(
      entry("a", "1.1"),
      entry("b", "2.1"),
      entry("c", "3.1"),
      entry("d", "4.2"),
    ), iter)
  }

  test("week1_day5_task1_merge_2") {
    val i1 = MemTableIterator(List(
      entry("a", "1.1"),
      entry("b", "2.1"),
      entry("c", "3.1"),
    ).iterator)
    val i2 = MemTableIterator(List(
      entry("a", "1.2"),
      entry("b", "2.2"),
      entry("c", "3.2"),
      entry("d", "4.2"),
    ).iterator)
    val iter = TwoMergeIterator(i2, i1)
    checkIterator(List(
      entry("a", "1.2"),
      entry("b", "2.2"),
      entry("c", "3.2"),
      entry("d", "4.2"),
    ), iter)
  }

  test("week1_day5_task1_merge_3") {
    val i1 = MemTableIterator(List(
      entry("a", "1.1"),
      entry("b", "2.1"),
      entry("c", "3.1"),
    ).iterator)
    val i2 = MemTableIterator(List(
      entry("b", "2.2"),
      entry("c", "3.2"),
      entry("d", "4.2"),
    ).iterator)
    val iter = TwoMergeIterator(i2, i1)
    checkIterator(List(
      entry("a", "1.1"),
      entry("b", "2.2"),
      entry("c", "3.2"),
      entry("d", "4.2"),
    ), iter)
  }

  test("week1_day5_task1_merge_4") {
    val i1 = MemTableIterator(List().iterator)
    val i2 = MemTableIterator(List(
      entry("b", "2.2"),
      entry("c", "3.2"),
      entry("d", "4.2"),
    ).iterator)
    val iter = TwoMergeIterator(i1, i2)
    checkIterator(List(
      entry("b", "2.2"),
      entry("c", "3.2"),
      entry("d", "4.2"),
    ), iter)
  }

  test("week1_day5_task1_merge_5") {
    val i1 = MemTableIterator(List().iterator)
    val i2 = MemTableIterator(List(
      entry("b", "2.2"),
      entry("c", "3.2"),
      entry("d", "4.2"),
    ).iterator)
    val iter = TwoMergeIterator(i2, i1)
    checkIterator(List(
      entry("b", "2.2"),
      entry("c", "3.2"),
      entry("d", "4.2"),
    ), iter)
  }

  test("week1_day5_task1_merge_6") {
    val i1 = MemTableIterator(List().iterator)
    val i2 = MemTableIterator(List().iterator)
    val iter = TwoMergeIterator(i2, i1)
    checkIterator(List(), iter)
  }
}
