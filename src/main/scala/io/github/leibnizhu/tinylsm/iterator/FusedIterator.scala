package io.github.leibnizhu.tinylsm.iterator

/**
 * 主要用于包装异常处理
 * TODO 优化泛型声明
 *
 * @param iter 要包装的 StorageIterator 迭代器
 * @tparam K key类型
 * @tparam V value类型
 */
class FusedIterator[K, V](val iter: StorageIterator[K, V])
  extends StorageIterator[K, V] {
  // 是否已经抛出异常
  private var errorThrown: Boolean = false

  override def key(): K = {
    if (!isValid) {
      throw new IllegalStateException("Iterator is invalid")
    }
    iter.key()
  }

  override def value(): V = {
    if (!isValid) {
      throw new IllegalStateException("Iterator is invalid")
    }
    iter.value()
  }

  override def isValid: Boolean = {
    !errorThrown && iter.isValid
  }

  override def next(): Unit = {
    // 已经发生过错误的禁止next()
    if (errorThrown) {
      throw new IllegalStateException(" This Iterator threw exception...")
    }
    // 包装的迭代器不可用时禁止next()
    if (iter.isValid) {
      try {
        iter.next()
      } catch {
        case t: Throwable =>
          errorThrown = true
          throw t
      }
    }
  }

  override def numActiveIterators(): Int = iter.numActiveIterators()
}
