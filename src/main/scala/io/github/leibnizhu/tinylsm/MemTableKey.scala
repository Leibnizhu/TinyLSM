package io.github.leibnizhu.tinylsm

import io.github.leibnizhu.tinylsm.utils.{Bound, Excluded, Included, Unbounded}

import java.util
import scala.util.hashing.MurmurHash3

case class MemTableKey(bytes: Array[Byte], ts: Long = 0L) extends Comparable[MemTableKey] {
  def isEmpty: Boolean = bytes.isEmpty

  def nonEmpty: Boolean = bytes.nonEmpty

  def length: Int = bytes.length

  override def compareTo(other: MemTableKey): Int = {
    val bc = util.Arrays.compare(this.bytes, other.bytes)
    if (bc == 0) {
      if (other.ts < this.ts) {
        // other更老，则this更小排更前面
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

  def keyHash(): Int = MurmurHash3.seqHash(this.bytes)

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
  def applyForTest(key: String): MemTableKey = MemTableKey(key.getBytes)
}