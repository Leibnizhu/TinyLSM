package io.github.leibnizhu.tinylsm.utils

import io.github.leibnizhu.tinylsm.*
import io.github.leibnizhu.tinylsm.MemTableKey.{TS_RANGE_BEGIN, TS_RANGE_END}

/**
 * 定义key边界相关的类
 */
sealed class Bound

object Bound {
  def apply(boundType: String, boundKey: String): Bound = boundType.toLowerCase match
    case "unbounded" => Unbounded()
    case "excluded" => Excluded(boundKey)
    case "included" => Included(boundKey)
    case _ => throw new IllegalArgumentException(s"Unsupported bound type: $boundType")

  //  def withBeginTs(bound: Bound): Bound = replaceTs(bound, MemTableKey.TS_RANGE_BEGIN)
  def withBeginTs(bound: Bound): Bound = bound match
    case Included(k: Key) => Included(MemTableKey.replaceTs(k, TS_RANGE_BEGIN))
    case Excluded(k: Key) => Excluded(MemTableKey.replaceTs(k, TS_RANGE_END))
    case Bounded(k: Key, i: Boolean) => Bounded(MemTableKey.replaceTs(k, TS_RANGE_BEGIN), i)
    case b: Bound => b

  //  def withEndTs(bound: Bound): Bound = replaceTs(bound, MemTableKey.TS_RANGE_END)
  def withEndTs(bound: Bound): Bound = bound match
    case Included(k: Key) => Included(MemTableKey.replaceTs(k, TS_RANGE_END))
    case Excluded(k: Key) => Excluded(MemTableKey.replaceTs(k, TS_RANGE_BEGIN))
    case Bounded(k: Key, i: Boolean) => Bounded(MemTableKey.replaceTs(k, TS_RANGE_END), i)
    case b: Bound => b

  private def replaceTs(bound: Bound, ts: Long): Bound = bound match
    case Included(k: Key) => Included(MemTableKey.replaceTs(k, ts))
    case Excluded(k: Key) => Excluded(MemTableKey.replaceTs(k, ts))
    case Bounded(k: Key, i: Boolean) => Bounded(MemTableKey.replaceTs(k, ts), i)
    case b: Bound => b
}

/**
 * 无边界
 */
case class Unbounded() extends Bound

/**
 * 有边界
 *
 * @param bound     边界值
 * @param inclusive 是否包含边界值
 */
class Bounded(val bound: Key, val inclusive: Boolean) extends Bound

object Bounded {
  def apply(bound: Key, inclusive: Boolean): Bounded = {
    new Bounded(bound, inclusive)
  }

  def unapply(bounded: Bounded): Option[(Key, Boolean)] = {
    if (bounded == null) {
      None
    } else {
      Some(bounded.bound, bounded.inclusive)
    }
  }
}

/**
 * 包含边界值的边界
 *
 * @param b 边界值
 */
case class Included(b: Key) extends Bounded(b, true)

object Included {
  def apply(str: String): Included = Included(RawKey(str.getBytes))

  def apply(str: String, ts: Long): Included = Included(MemTableKey(str.getBytes, ts))
}

/**
 * 不包含边界值的边界
 *
 * @param b 边界值
 */
case class Excluded(b: Key) extends Bounded(b, false)

object Excluded {
  def apply(str: String): Excluded = Excluded(RawKey(str.getBytes))

  def apply(str: String, ts: Long): Excluded = Excluded(MemTableKey(str.getBytes, ts))
}
