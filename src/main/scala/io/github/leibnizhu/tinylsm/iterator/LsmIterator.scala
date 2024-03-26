package io.github.leibnizhu.tinylsm.iterator

import io.github.leibnizhu.tinylsm.*
import io.github.leibnizhu.tinylsm.utils.{Bound, Excluded, Included}

import scala.util.control.Breaks.breakable

/**
 * 用于LSM的遍历，主要封装了已删除元素的处理逻辑
 *
 * @param innerIter LsmIteratorInner内部迭代器
 * @param endBound  遍历的key上界
 */
class LsmIterator(val innerIter: LsmIteratorInner, endBound: Bound, readTs: Long) extends StorageIterator[RawKey] {
  // LsmIterator本身是否可用，
  private var isSelfValid = innerIter.isValid
  private var prevKey: Array[Byte] = Array()
  // 跳过前面已删除的元素
  moveToKey()

  override def key(): RawKey = innerIter.key().rawKey()

  override def value(): MemTableValue = innerIter.value()

  override def isValid: Boolean = isSelfValid

  override def next(): Unit = {
    innerNext()
    moveToKey()
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
      case Included(r) => isSelfValid = key().compareTo(r.rawKey()) <= 0
      case Excluded(r) => isSelfValid = key().compareTo(r.rawKey()) < 0
      case _ =>
  }

  /**
   * 跳过迭代器里的空值，以及同key的其他时间戳/版本(如果)
   */
  private def moveToKey(): Unit = {
    // 外层循环是找到下个能用的key
    while (true) {
      // 跳过已经迭代过的key
      while (innerIter.isValid && innerIter.key().bytes.sameElements(prevKey)) {
        println(s"moveToKey with prevKey=${new String(prevKey)}, curKey: ${innerIter.key()}")
        innerNext()
      }
      // 如果迭代器不可用则直接跳过
      if (!innerIter.isValid) {
        return
      }
      prevKey = innerIter.key().bytes.clone()

      // 跳过比当前要读的时间戳/版本更新的版本
      while (innerIter.isValid && innerIter.key().rawKey().equals(prevKey)
        && innerIter.key().ts > readTs) {
        println(s"moveToKey with prevKey=${new String(prevKey)}, curKey: ${innerIter.key()}, readTs=${readTs} ")
        innerNext()
      }
      // 如果迭代器不可用则直接跳过
      if (!innerIter.isValid) {
        return
      }

      if (innerIter.key().rawKey().equals(prevKey)) {
        // 当前key和prevKey相同，说明当前key是有满足 readTs 的版本要的value
        if (!innerIter.value().sameElements(DELETE_TOMBSTONE)) {
          // 遇到不是删除的就是要查的数据了，否则继续下个key
          return
        }
      }
      // 否则如果当前key已经和prevKey不同，说明当前key的所有版本都不满足 readTs 的版本要求，需要继续找下个key
    }
  }

  override def numActiveIterators(): Int = innerIter.numActiveIterators()
}
