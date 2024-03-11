package io.github.leibnizhu.tinylsm.utils

import io.github.leibnizhu.tinylsm.*

/**
 * 定义key边界相关的类
 */
sealed class Bound

object Bound {
  def apply(boundType: String, boundKey: String): Bound = boundType.toLowerCase match
    case "unbounded" => Unbounded()
    case "excluded" => Excluded(boundKey)
    case "included" => Included(boundKey)
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
class Bounded(val bound: MemTableKey, val inclusive: Boolean) extends Bound

object Bounded {
  def apply(bound: MemTableKey, inclusive: Boolean): Bounded = {
    new Bounded(bound, inclusive)
  }

  def unapply(bounded: Bounded): Option[(MemTableKey, Boolean)] = {
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
case class Included(b: MemTableKey) extends Bounded(b, true)

object Included {
  def apply(str: String): Included = Included(MemTableKey(str.getBytes))
}

/**
 * 不包含边界值的边界
 *
 * @param b 边界值
 */
case class Excluded(b: MemTableKey) extends Bounded(b, false)

object Excluded {
  def apply(str: String): Excluded = Excluded(MemTableKey(str.getBytes))
}
