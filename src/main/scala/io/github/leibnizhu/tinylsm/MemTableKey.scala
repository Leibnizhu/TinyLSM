package io.github.leibnizhu.tinylsm

import io.github.leibnizhu.tinylsm.utils.{Bound, Excluded, Included, Unbounded}

import java.util
import scala.util.hashing.MurmurHash3

case class MemTableKey(bytes: Array[Byte], ts: Long = TS_DEFAULT) extends Comparable[MemTableKey] with Key {

  def rawLength: Int = bytes.length + SIZE_OF_LONG

  override def compareTo(other: MemTableKey): Int = {
    val bc = util.Arrays.compare(this.bytes, other.bytes)
    if (bc == 0) {
      if (this.ts > other.ts) {
        // this时间戳更大，即版本更新，排更前面
        -1
      } else if (other.ts == this.ts) {
        0
      } else {
        1
      }
    } else {
      bc
    }
  }

  def compareOnlyKeyTo(other: MemTableKey): Int =
    util.Arrays.compare(this.bytes, other.bytes)

  override def hashCode(): Int = MurmurHash3.seqHash(this.ts +: this.bytes)

  override def equals(other: Any): Boolean = other match
    case MemTableKey(bs, ts) => bs.sameElements(this.bytes) && ts == this.ts
    case _ => false

  override def toString: String = s"${new String(bytes)}@$ts"

  /**
   * 如果有边界值，则大于(等于)边界
   *
   * @param lower 下边界
   * @return 是否满足下边界
   */
  def lowerBound(lower: Bound): Boolean = lower match
    case Unbounded() => true
    case Excluded(bound: MemTableKey) => this.compareTo(bound) > 0
    case Included(bound: MemTableKey) => this.compareTo(bound) >= 0
    case _ => false

  /**
   * 如果有边界值，则小于(等于)边界
   *
   * @param upper 上边界
   * @return 是否满足上边界
   */
  def upperBound(upper: Bound): Boolean = upper match
    case Unbounded() => true
    case Excluded(bound: MemTableKey) => this.compareTo(bound) < 0
    case Included(bound: MemTableKey) => this.compareTo(bound) <= 0
    case _ => false
}

object MemTableKey {
  val TS_DEFAULT: Long = 0L
  val TS_MAX: Long = Long.MaxValue
  val TS_MIN: Long = 0L
  // 时间戳越大，即版本越新，key对比起来是越小
  val TS_RANGE_BEGIN: Long = TS_MAX
  val TS_RANGE_END: Long = TS_MIN

  def applyForTest(key: String): MemTableKey = MemTableKey(key.getBytes, TS_DEFAULT)

  def withBeginTs(key: Key): MemTableKey = MemTableKey(key.bytes, TS_RANGE_BEGIN)

  def withBeginTs(bytes: Array[Byte]): MemTableKey = MemTableKey(bytes, TS_RANGE_BEGIN)

  def withEndTs(key: Key): MemTableKey = MemTableKey(key.bytes, TS_RANGE_END)

  def withEndTs(bytes: Array[Byte]): MemTableKey = MemTableKey(bytes, TS_RANGE_END)

  def replaceTs(key: Key, ts: Long): MemTableKey = MemTableKey(key.bytes, ts)
}