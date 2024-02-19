package io.github.leibnizhu.tinylsm

trait StorageIterator[K, V] {

  /**
   * 当前key
   */
  def key(): K

  /**
   * 当前值
   *
   * @return
   */
  def value(): V

  /**
   *
   * @return 是否可用（有下一个元素）
   */
  def isValid: Boolean

  /**
   * 移动游标
   */
  def next(): Unit

  /**
   * 当前迭代器的潜在活动迭代器的数量。
   */
  def numActiveIterators(): Int = 1
}

class MemTableIterator(val iterator: Iterator[MemTableEntry])
  extends MemTableStorageIterator {
  private var currentEntry: MemTableEntry = _

  /**
   * 当前key
   */
  override def key(): Array[Byte] = {
    assert(currentEntry != null, "Plz call next() first")
    currentEntry.getKey.bytes
  }

  /**
   * 当前值
   *
   * @return
   */
  override def value(): MemTableValue = {
    assert(currentEntry != null, "Plz call next() first")
    currentEntry.getValue
  }

  /**
   *
   * @return 是否可用（有下一个元素）
   */
  override def isValid: Boolean = {
    iterator.hasNext
  }

  /**
   * 移动游标
   */
  override def next(): Unit = {
    currentEntry = iterator.next()
  }
}

/**
 * 合并多个迭代器
 * @param iterators 所有迭代器，由新到旧
 * @param curIterator 当前迭代器
 */
class MergeIterator(val iterators: List[MemTableStorageIterator],
                    var curIterator: Option[MemTableStorageIterator] = None)
  extends MemTableStorageIterator {

  /**
   * 当前key
   */
  override def key(): Array[Byte] = {
    null
  }

  /**
   * 当前值
   *
   * @return
   */
  override def value(): MemTableValue = {
    null
  }

  /**
   *
   * @return 是否可用（有下一个元素）
   */
  override def isValid: Boolean = {
    false
  }

  /**
   * 移动游标
   */
  override def next(): Unit = {

  }
}
