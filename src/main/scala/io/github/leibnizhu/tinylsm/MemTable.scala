package io.github.leibnizhu.tinylsm


import io.github.leibnizhu.tinylsm.iterator.MemTableIterator
import io.github.leibnizhu.tinylsm.utils.*

import java.io.File
import java.util
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicInteger
import scala.jdk.CollectionConverters.*
import scala.util.hashing.MurmurHash3

/**
 * MemTable。
 * 其大小增长到 LsmStorageOptions.targetSstSize 之后，需要冻结，并flush到磁盘
 *
 * @param id              MemTable唯一标识，应该是自增的，或至少是单调增的
 * @param map             内部存储的Map，Key用了ByteArrayKey，自定义了有效的equals方法；否则如果用Array[Byte]，equals和hashCode使用的是对象ID
 * @param wal             可选的WriteAheadLog
 * @param approximateSize 记录当前的预估大小
 */
case class MemTable(
                     id: Int,
                     map: ConcurrentSkipListMap[ByteArrayKey, MemTableValue],
                     wal: Option[WriteAheadLog],
                     approximateSize: AtomicInteger) {

  /**
   * 按key获取
   *
   * @param key key
   * @return 不存在Key则为None，如果put进来是空Array返回也是空Array，注意区分两种
   */
  def get(key: MemTableKey): Option[MemTableValue] = {
    Option(map.get(ByteArrayKey(key)))
  }

  /**
   * 插入或更新
   *
   * @param key   key
   * @param value 如果要执行delete操作，可以传入空Array
   */
  def put(key: MemTableKey, value: MemTableValue): Unit = {
    val estimateSize = key.length + (if (value == null) 0 else value.length)
    map.put(ByteArrayKey(key), value)
    approximateSize.addAndGet(estimateSize)
    wal.foreach(_wal => _wal.put(key, value))
  }

  /**
   * 按指定的上下界返回满足条件的Map迭代器
   *
   * @param lower 下界
   * @param upper 上界
   * @return MemTableIterator
   */
  def scan(lower: Bound, upper: Bound): MemTableIterator = (lower, upper) match
    case (Unbounded(), Unbounded()) =>
      new MemTableIterator(map.entrySet().iterator().asScala)
    case (Unbounded(), Bounded(r: MemTableKey, inclusive: Boolean)) =>
      new MemTableIterator(map.headMap(ByteArrayKey(r), inclusive).entrySet().iterator().asScala)
    case (Bounded(l: MemTableKey, inclusive: Boolean), Unbounded()) =>
      new MemTableIterator(map.tailMap(ByteArrayKey(l), inclusive).entrySet().iterator().asScala)
    case (Bounded(l: MemTableKey, il: Boolean), Bounded(r: MemTableKey, ir: Boolean)) =>
      new MemTableIterator(map.subMap(ByteArrayKey(l), il, ByteArrayKey(r), ir).entrySet().iterator().asScala)
    case (_, _) => null

  def flush(builder: SsTableBuilder): Unit = {
    map.forEach((k, v) => builder.add(k.bytes, v))
  }
}

object MemTable {
  /**
   *
   * @param id      MemTable唯一标识，应该是自增的，或至少是单调增的
   * @param walPath WriteAheadLog的存储路径；默认不做WAL
   * @return MemTable实例
   */
  def apply(id: Int, walPath: Option[File] = None): MemTable = new MemTable(id,
    new ConcurrentSkipListMap[ByteArrayKey, MemTableValue](),
    walPath.map(p => WriteAheadLog(p)),
    AtomicInteger(0))
}

case class ByteArrayKey(val bytes: MemTableKey) extends Comparable[ByteArrayKey] {
  override def compareTo(other: ByteArrayKey): Int = {
    byteArrayCompare(this.bytes, other.bytes)
  }

  override def hashCode(): Int = byteArrayHash(this.bytes)

  override def equals(other: Any): Boolean = other match
    case ByteArrayKey(bs) => bs.sameElements(this.bytes)
    case _ => false

  override def toString: String = bytes.mkString("[", ", ", "]")

  /**
   * 如果有边界值，则大于(等于)边界
   *
   * @param lower 下边界
   * @return 是否满足下边界
   */
  def lowerBound(lower: Bound): Boolean = lower match
    case Unbounded() => true
    case Excluded(bound: MemTableKey) => this.compareTo(ByteArrayKey(bound)) > 0
    case Included(bound: MemTableKey) => this.compareTo(ByteArrayKey(bound)) >= 0
    case _ => false

  /**
   * 如果有边界值，则小于(等于)边界
   *
   * @param upper 上边界
   * @return 是否满足上边界
   */
  def upperBound(upper: Bound): Boolean = upper match
    case Unbounded() => true
    case Excluded(bound: MemTableKey) => this.compareTo(ByteArrayKey(bound)) < 0
    case Included(bound: MemTableKey) => this.compareTo(ByteArrayKey(bound)) <= 0
    case _ => false
}
