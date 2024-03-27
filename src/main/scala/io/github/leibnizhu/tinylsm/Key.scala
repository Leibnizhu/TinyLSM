package io.github.leibnizhu.tinylsm

import java.util
import scala.util.hashing.MurmurHash3

trait Key {
  def bytes: Array[Byte]

  def rawKey(): RawKey = RawKey(this.bytes)

  def isEmpty: Boolean = bytes.isEmpty

  def nonEmpty: Boolean = bytes.nonEmpty

  def length: Int = bytes.length

  def keyHash(): Int = MurmurHash3.seqHash(this.bytes)
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