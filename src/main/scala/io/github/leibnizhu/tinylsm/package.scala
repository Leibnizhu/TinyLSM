package io.github.leibnizhu

import io.github.leibnizhu.tinylsm.iterator.StorageIterator

import java.util.Comparator
import scala.util.hashing.MurmurHash3

package object tinylsm {
  // 16位无符号整型，u16，即short的无符号版，所占的byte数量
  val SIZE_OF_U16 = 2
  // Int的byte数
  val SIZE_OF_INT = 4
  // Long 的byte数
  val SIZE_OF_LONG = 8

  // 已删除的key对应的value
  val DELETE_TOMBSTONE: Array[Byte] = Array()


  type MemTableValue = Array[Byte]
  type MemTableEntry = java.util.Map.Entry[MemTableKey, MemTableValue]
  type Level = (Int, List[Int])

  implicit val keyComparator: Comparator[MemTableKey] = new java.util.Comparator[MemTableKey]() {
    override def compare(o1: MemTableKey, o2: MemTableKey): Int =
      java.util.Arrays.compare(o1.bytes, o2.bytes)
  }

  implicit val byteArrayComparator: Comparator[Array[Byte]] = new java.util.Comparator[Array[Byte]]() {
    override def compare(o1: Array[Byte], o2: Array[Byte]): Int =
      java.util.Arrays.compare(o1, o2)
  }

  def byteArrayHash(bytes: Array[Byte]): Int = {
    MurmurHash3.seqHash(bytes)
  }

  def partitionPoint[T](items: Seq[T], target: T => Boolean): Int = {
    // 二分查找，找到最后（数组的右边满足 target 的索引
    var low = 0
    var high = items.length - 1
    var result = 0
    while (low <= high) {
      val mid = (high + low) / 2
      val matched = target(items(mid))
      if (matched) {
        // 满足条件时，先存储起来，有可能是最终结果，low尝试右移
        result = mid
        low = mid + 1
      } else {
        //当前mid不满足条件，所以右边界应该比mid还小，减一
        high = mid - 1
      }
    }
    result
  }
}