package io.github.leibnizhu.tinylsm.iterator

import io.github.leibnizhu.tinylsm.{Key, MemTableValue}

import scala.math.Ordering.Implicits.infixOrderingOps

/**
 * 合并两个迭代器，比起MergeIterator，逻辑更简单，对于只有两个迭代器的场景，效率更高
 *
 * @param a 优先迭代的迭代器
 * @param b 另一个迭代器，优先级低
 * @tparam K key类型
 * @tparam A a迭代器类型
 * @tparam B b迭代器类型
 */
class TwoMergeIterator[K <: Comparable[K] with Key, A <: StorageIterator[K], B <: StorageIterator[K]]
(val a: A, val b: B) extends StorageIterator[K] {
  skipB()
  private var isUseA: Boolean = useA()

  override def key(): K = chooseIter().key()

  override def value(): MemTableValue = chooseIter().value()

  override def isValid: Boolean = chooseIter().isValid

  override def next(): Unit = {
    chooseIter().next()
    skipB()
    isUseA = useA()
  }

  private def chooseIter(): StorageIterator[K] = if (isUseA) a else b

  private def useA(): Boolean = {
    if (!a.isValid) {
      // a不可用的话只能用b
      false
    } else if (!b.isValid) {
      // a可用、b不可用时，直接用a
      true
    } else {
      // a b 都可用，那么用key较小的。调用 useA() 之前调用 skipB() 则不会出现两个key相等的情况
      a.key() < b.key()
    }
  }

  private def skipB(): Unit = {
    // 如果 a b 都可用且key相同，那么优先用a的，将b的跳过
    if (a.isValid && b.isValid && a.key().equals(b.key())) {
      b.next()
    }
  }

  override def numActiveIterators(): Int = a.numActiveIterators() + b.numActiveIterators()
}
