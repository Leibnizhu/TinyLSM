package io.github.leibnizhu.tinylsm

import io.github.leibnizhu.tinylsm.utils.{Bound, Excluded, Included, Unbounded}

import java.util
import scala.util.hashing.MurmurHash3

trait Key {
  def bytes: Array[Byte]

  def rawKey(): RawKey = RawKey(this.bytes)

  def isEmpty: Boolean = bytes.isEmpty

  def nonEmpty: Boolean = bytes.nonEmpty

  def length: Int = bytes.length

  def keyHash(): Int = MurmurHash3.seqHash(this.bytes)

  def prefixUpperEdge(): Bound = {
    val lastAddIndex = bytes.lastIndexWhere(_ != Byte.MaxValue)
    if (lastAddIndex > 0) {
      val newBytes = bytes.clone()
      newBytes(lastAddIndex) = (newBytes(lastAddIndex) + 1).toByte
      Excluded(MemTableKey.withEndTs(newBytes))
    } else {
      Unbounded()
    }
  }
  
  def prefixRange(): (Bound,Bound) = {
    val lower = Included(MemTableKey.withBeginTs(this))
    val upper = prefixUpperEdge()
    (lower, upper)
  }
}

case class RawKey(bytes: Array[Byte]) extends Comparable[RawKey] with Key {
  override def compareTo(other: RawKey): Int = util.Arrays.compare(this.bytes, other.bytes)

  override def hashCode(): Int = keyHash()

  override def equals(other: Any): Boolean = other match
    case RawKey(bs) => bs.sameElements(this.bytes)
    case bs: Array[Byte] => bs.sameElements(this.bytes)
    case _ => false

  override def rawKey(): RawKey = this

  override def toString: String = new String(bytes)
}

object RawKey {
  def fromString(str: String): RawKey = RawKey(str.getBytes)
}