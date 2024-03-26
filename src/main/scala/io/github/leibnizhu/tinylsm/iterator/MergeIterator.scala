package io.github.leibnizhu.tinylsm.iterator

import io.github.leibnizhu.tinylsm.*

import java.util.PriorityQueue
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.*
import scala.util.hashing.MurmurHash3

/**
 * 合并多个迭代器，迭代顺序是按key升序。
 * 如果多个迭代器有相同的key，那么取最新（下标最小）的迭代器，且其他同key的迭代器迭代下一个元素
 * 在实现上，使用一个小顶堆，大小和迭代器数量相同，初始时将所有可用的迭代器入堆
 * 每次迭代（next()）时，如果堆中有相同的key的其他迭代器，只保留堆顶的(也就是保留最新的)，其他移除
 * 而堆顶是当前在用的，所以堆顶的迭代器需要迭代，同时需要重新入堆以更新其位置
 *
 * @param iterHeap 所有迭代器构成的小顶堆
 * @param curItr   当前用到的迭代器
 */
class MergeIterator[I <: StorageIterator[MemTableKey]](
                                                        val iterHeap: PriorityQueue[HeapWrapper],
                                                        var curItr: Option[HeapWrapper] = None
                                                      ) extends StorageIterator[MemTableKey] {

  /**
   * 当前key
   */
  override def key(): MemTableKey = {
    curItr.map(_.key()).orNull
  }

  /**
   * 当前值
   *
   * @return
   */
  override def value(): MemTableValue = {
    curItr.map(_.itr.value()).orNull
  }

  /**
   *
   * @return 是否可用（有下一个元素）
   */
  override def isValid: Boolean = {
    curItr.map(_.isValid).getOrElse(false)
  }

  /**
   * 移动游标
   */
  override def next(): Unit = {
    val curKey = key()
    val curIter = curItr.get

    // 如果堆中有相同的key的其他迭代器，只保留堆顶的(也就是保留最新的)，其他移除
    val toRetain = ArrayBuffer[HeapWrapper]()
    // 遍历堆中所有迭代器，把不相同key的迭代器记录到 toRetain
    val itersItr = iterHeap.iterator()
    while (itersItr.hasNext) {
      val innerIter = itersItr.next()
      if (curIter != innerIter && curKey.equals(innerIter.key())) {
        // 如果不保留，则要remove；如果要保留，因为next了，也要remove后重新offer，才能更新权重
        itersItr.remove()
        // 迭代器已经remove，如果 next() 异常则直接退出，所以这里没 try-catch
        innerIter.next()
        if (innerIter.isValid) {
          // 不能直接offer给堆，否则会 ConcurrentModificationException
          toRetain.addOne(innerIter)
        }
        // 不可用的话，就不处理了，已经remove
      }
    }
    toRetain.foreach(iterHeap.offer)

    curIter.next()
    // 当前迭代器往下、更新位置，并更新当前迭代器
    if (!curIter.isValid) {
      // 当前迭代器不可用了，从堆中拿最新的
      curItr = Option(iterHeap.poll())
    } else {
      // 当前迭代器可用，继续迭代，并更新在堆的位置
      iterHeap.remove(curIter)
      iterHeap.offer(curIter)
      // 更新当前的迭代器为堆顶迭代器
      curItr = Option(iterHeap.poll())
    }
  }

  override def numActiveIterators(): Int =
    iterHeap.iterator().asScala.map(_.itr.numActiveIterators()).sum
      + curItr.map(_.itr.numActiveIterators()).getOrElse(0)
}

object MergeIterator {
  def apply[I <: StorageIterator[MemTableKey]](iterators: List[I]): MergeIterator[I] = {
    val heap = new PriorityQueue[HeapWrapper](Math.max(1, iterators.length))
    if (iterators.isEmpty) {
      new MergeIterator(heap, None)
    } else if (iterators.forall(!_.isValid)) {
      // 所有迭代器都不可用，那么当前迭代器用第一个就可以
      new MergeIterator(heap, Some(HeapWrapper(0, iterators.head)))
    } else {
      // 否则所有迭代器入堆，取堆顶作为当前迭代器
      for ((itr, index) <- iterators.zipWithIndex) {
        if (itr.isValid) {
          heap.offer(HeapWrapper(index, itr))
        }
      }
      new MergeIterator(heap, Some(heap.poll()))
    }
  }
}

/**
 * 包装一个MemTable迭代器，用到堆中
 * 提供了按key+MemTable层级排序的功能，实现配套方法（compareTo/hashCode/equals）
 *
 * @param index 当前MemTable迭代器的序号，越小越新，0对应未freeze的MemTable，1之后是已freeze的MemTable
 * @param itr   MemTable迭代器
 */
case class HeapWrapper(index: Int, itr: StorageIterator[MemTableKey]) extends Comparable[HeapWrapper] {

  def key(): MemTableKey = itr.key()

  def isValid: Boolean = itr.isValid

  def next(): Unit = itr.next()

  override def compareTo(other: HeapWrapper): Int = {
    // 先按key进行比较，同key的时候更新（index更小的）的优先
    val keyCompare = this.itr.key().compareTo(other.itr.key())
    if (keyCompare == 0) {
      // 小的index就是更新的迭代器
      this.index - other.index
    } else {
      keyCompare
    }
  }

  override def hashCode(): Int = MurmurHash3.seqHash(index +: itr.key().bytes)

  override def equals(other: Any): Boolean = other match
    case otherHw: HeapWrapper => otherHw.index == this.index &&
      otherHw.itr.key().equals(this.itr.key())
    case _ => false

  override def toString: String =
    if (itr.isValid) s"Index=$index, current: ${itr.key()} => ${new String(itr.value())}})"
    else s"Index=$index, current invalid"
}