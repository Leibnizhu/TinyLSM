package io.github.leibnizhu.tinylsm.iterator

import io.github.leibnizhu.tinylsm.{DELETE_TOMBSTONE, Key, MemTableValue}

import java.util.StringJoiner
import scala.collection.mutable.ArrayBuffer

/**
 * LSM存储相关的迭代器trait
 * 调用顺序：isValid -> key/value -> next
 *
 * @tparam K key类型
 */
trait StorageIterator[K <: Comparable[K] with Key] {

  /**
   * 当前key
   */
  def key(): K

  /**
   * 当前值
   *
   * @return
   */
  def value(): MemTableValue

  def deletedValue(): Boolean = value().sameElements(DELETE_TOMBSTONE)

  /**
   * 这里可用指可调用key() value() next()
   * 千万注意这是不同于hasNext的语义
   * 迭代器初始化之后应该是可用状态，无需调用next()方法已经指向第一个元素
   *
   * @return 是否可用
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

  final def joinAllKeyValue(sj: StringJoiner): StringJoiner = {
    while (isValid) {
      (key(), value()) match
        case (curKey: Key, curValue: MemTableValue) =>
          sj.add(curKey.toString)
          sj.add(new String(curValue))
        case _ | null =>
      next()
    }
    sj
  }

  final def collectEntries(): List[(Key, MemTableValue)] = {
    val buffer = new ArrayBuffer[(Key, MemTableValue)]
    while (isValid) {
      (key(), value()) match
        case (curKey: Key, curValue: MemTableValue) =>
          buffer += ((curKey, curValue))
        case _ | null =>
      next()
    }
    buffer.toList
  }
}
