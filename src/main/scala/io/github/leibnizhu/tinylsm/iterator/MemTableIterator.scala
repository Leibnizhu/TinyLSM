package io.github.leibnizhu.tinylsm.iterator

import io.github.leibnizhu.tinylsm.{MemTableEntry, MemTableKey, MemTableValue}

/**
 * 用于遍历一个MemTable的迭代器
 *
 * @param iterator MemTable内部数据MemTableEntry的迭代器
 */
class MemTableIterator(val iterator: Iterator[MemTableEntry], val memTableId: Int = 0)
  extends StorageIterator[MemTableKey] {
  // 记录当前迭代到的Entry
  private var currentEntry: MemTableEntry = if (iterator.hasNext) iterator.next() else null

  /**
   * 当前key
   */
  override def key(): MemTableKey = {
    currentEntry.getKey
  }

  /**
   * 当前值
   *
   * @return
   */
  override def value(): MemTableValue = {
    currentEntry.getValue
  }

  /**
   *
   * @return 是否可用（可调用key() value() next()）不同于hasNext的语义
   */
  override def isValid: Boolean = {
    currentEntry != null
  }

  /**
   * 移动游标
   */
  override def next(): Unit = {
    if (iterator.hasNext) {
      currentEntry = iterator.next()
    } else {
      currentEntry = null
    }
  }

  override def toString: String = s"MemTableIterator($memTableId)"
}
