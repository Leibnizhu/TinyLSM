package io.github.leibnizhu.tinylsm.mvcc

import io.github.leibnizhu.tinylsm.iterator.StorageIterator
import io.github.leibnizhu.tinylsm.mvcc.TxnLocalIterator.extractCurItem
import io.github.leibnizhu.tinylsm.utils.{Bound, Bounded, Unbounded}
import io.github.leibnizhu.tinylsm.{MemTableKey, MemTableValue, RawKey}

import java.util
import java.util.Map
import java.util.concurrent.ConcurrentSkipListMap
import scala.jdk.CollectionConverters.*

/**
 * 用于迭代 Transaction 内部存储 TreeMap
 *
 * @param iter Transaction 内部存储 TreeMap 的迭代器
 * @param item 当前迭代到的k-v对
 */
class TxnLocalIterator(
                        /// skipmap 迭代器
                        iter: Iterator[util.Map.Entry[RawKey, MemTableValue]],
                        /// 存储当前的 key-value 对.
                        var item: (RawKey, MemTableValue),
                      ) extends StorageIterator[RawKey] {
  override def key(): RawKey = item._1

  override def value(): MemTableValue = item._2

  override def isValid: Boolean = item._1.nonEmpty

  override def next(): Unit = {
    val curItem = extractCurItem(iter)
    this.item = curItem
  }
}

object TxnLocalIterator {
  def apply(map: ConcurrentSkipListMap[RawKey, MemTableValue], lower: Bound, upper: Bound): TxnLocalIterator = {
    val innerIter = (lower, upper) match
      case (Unbounded(), Unbounded()) =>
        map.entrySet().iterator().asScala
      case (Unbounded(), Bounded(r: MemTableKey, inclusive: Boolean)) =>
        map.headMap(RawKey(r.bytes), inclusive).entrySet().iterator().asScala
      case (Bounded(l: MemTableKey, inclusive: Boolean), Unbounded()) =>
        map.tailMap(RawKey(l.bytes), inclusive).entrySet().iterator().asScala
      case (Bounded(l: MemTableKey, il: Boolean), Bounded(r: MemTableKey, ir: Boolean)) =>
        map.subMap(RawKey(l.bytes), il, RawKey(r.bytes), ir).entrySet().iterator().asScala
      case (_, _) => null
    val firstItem = extractCurItem(innerIter)
    new TxnLocalIterator(
      iter = innerIter,
      item = firstItem
    )
  }

  private def extractCurItem(innerIter: Iterator[util.Map.Entry[RawKey, MemTableValue]]) = {
    if (innerIter.hasNext) {
      val entry = innerIter.next()
      (entry.getKey, entry.getValue)
    } else {
      (RawKey(Array()), Array[Byte]())
    }
  }
}