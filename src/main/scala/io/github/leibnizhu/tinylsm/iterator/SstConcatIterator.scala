package io.github.leibnizhu.tinylsm.iterator

import io.github.leibnizhu.tinylsm.*
import io.github.leibnizhu.tinylsm.utils.{Bound, Excluded, Included, Unbounded}

import scala.jdk.CollectionConverters.*


/**
 * 用于遍历多个已排序的、key范围没有重叠的SST
 * 由于这样的特性，只要根据firstKey和lastKey可以快速定位
 *
 * @param current      当前的Sstable迭代器
 * @param nextSstIndex 下一个sst在ssTables的下标
 * @param ssTables     所有sst
 */
class SstConcatIterator(
                         var current: Option[SsTableIterator],
                         var nextSstIndex: Int,
                         ssTables: List[SsTable]
                       ) extends StorageIterator[MemTableKey] {

  override def key(): MemTableKey = current.map(_.key()).orNull

  override def value(): MemTableValue = current.map(_.value()).orNull

  override def isValid: Boolean = current.isDefined && current.get.isValid

  override def next(): Unit = {
    current.get.next()
    moveUntilValid()
  }

  def moveUntilValid(): Unit = {
    while (current.isDefined) {
      val iter = current.get
      if (iter.isValid) {
        // 找到可用迭代器了
        return
      }
      if (nextSstIndex >= ssTables.length) {
        // 找完所有迭代器，没有可用的了
        current = None
      } else {
        // 找下一个迭代器
        current = Some(SsTableIterator.createAndSeekToFirst(ssTables(nextSstIndex)))
        nextSstIndex += 1
      }
    }
  }
}

object SstConcatIterator {
  def createAndSeekToFirst(ssTables: List[SsTable]): SstConcatIterator = {
    checkSstValid(ssTables)
    if (ssTables.isEmpty) {
      return new SstConcatIterator(None, 0, ssTables)
    }
    val iter = new SstConcatIterator(Some(SsTableIterator.createAndSeekToFirst(ssTables.head)), 1, ssTables)
    iter.moveUntilValid()
    iter
  }

  def createAndSeekToKey(ssTables: List[SsTable], key: MemTableKey): SstConcatIterator = {
    checkSstValid(ssTables)
    val idx = partitionPoint(ssTables, sst => sst.firstKey.compareTo(key) <= 0)
    if (idx >= ssTables.length) {
      // 没找到包含key的sst
      return new SstConcatIterator(None, ssTables.length, ssTables)
    }
    val iter = new SstConcatIterator(Some(SsTableIterator.createAndSeekToKey(ssTables(idx), key)), idx + 1, ssTables)
    iter.moveUntilValid()
    iter
  }

  private def checkSstValid(ssTables: List[SsTable]): Unit = {
    for (sst <- ssTables) {
      assert(sst.firstKey.compareTo(sst.lastKey) <= 0)
    }
    if (ssTables.nonEmpty) {
      for (i <- 0 until ssTables.length - 1) {
        assert(ssTables(i).lastKey.compareTo(ssTables(i + 1).firstKey) < 0)
      }
    }
  }

  def createByLowerBound(ssTables: List[SsTable], lower: Bound): SstConcatIterator = lower match {
    // 没有左边界，则直接到最开始遍历
    case Unbounded() => SstConcatIterator.createAndSeekToFirst(ssTables)
    // 包含左边界，则可以跳到左边界的key开始遍历
    case Included(l: MemTableKey) => SstConcatIterator.createAndSeekToKey(ssTables, MemTableKey.withBeginTs(l))
    // 不包含左边界，则先跳到左边界的key，如果跳完之后实际的key等于左边界，由于不包含边界所以跳到下个值
    case Excluded(l: MemTableKey) =>
      val iter = SstConcatIterator.createAndSeekToKey(ssTables, MemTableKey.withBeginTs(l))
      if (iter.isValid && iter.key().rawKey().equals(l.rawKey())) {
        iter.next()
      }
      iter
    case _ => throw new IllegalArgumentException("Unsupported Bound type: " + lower.getClass)
  }
}