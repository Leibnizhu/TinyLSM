package io.github.leibnizhu.tinylsm.iterator

import io.github.leibnizhu.tinylsm.*
import io.github.leibnizhu.tinylsm.utils.{Bound, Excluded, Included}

/**
 * 用于LSM的遍历，主要封装了已删除元素的处理逻辑
 *
 * @param innerIter LsmIteratorInner内部迭代器
 * @param endBound  遍历的key上界
 */
class LsmIterator(val innerIter: LsmIteratorInner, endBound: Bound) extends MemTableStorageIterator {
  // LsmIterator本身是否可用，
  private var isSelfValid = innerIter.isValid
  // 跳过前面已删除的元素
  moveToNonDeleted()

  override def key(): MemTableKey = innerIter.key()

  override def value(): MemTableValue = innerIter.value()

  override def isValid: Boolean = isSelfValid

  override def next(): Unit = {
    innerNext()
    moveToNonDeleted()
  }

  /**
   * 包装的迭代器继续迭代
   */
  private def innerNext(): Unit = {
    innerIter.next()
    // 如果迭代器不可用则直接跳过
    if (!innerIter.isValid) {
      isSelfValid = false
      return
    }
    // 由于LsmIteratorInner 包含了MemTable和SST的迭代器，而SST的迭代器不支持上界
    // 所以还要检查下上界，如果到达上界则当前LsmIterator不可用
    endBound match
      case Included(r) => isSelfValid = byteArrayCompare(key(), r) <= 0
      case Excluded(r) => isSelfValid = byteArrayCompare(key(), r) < 0
      case _ =>
  }

  /**
   * 跳过迭代器里的空值
   */
  private def moveToNonDeleted(): Unit = {
    while (isSelfValid && innerIter.value().sameElements(DELETE_TOMBSTONE)) {
      innerNext()
    }
  }

  override def numActiveIterators(): Int = innerIter.numActiveIterators()
}
