package io.github.leibnizhu.tinylsm

/**
 * 定义key边界相关的类
 */
sealed class Bound;

/**
 * 无边界
 */
case class Unbounded() extends Bound;

/**
 * 有边界
 *
 * @param bound     边界值
 * @param inclusive 是否包含边界值
 */
class Bounded(val bound: Array[Byte], val inclusive: Boolean) extends Bound;

object Bounded {
  def apply(bound: Array[Byte], inclusive: Boolean): Bounded = {
    new Bounded(bound, inclusive)
  }

  def unapply(bounded: Bounded): Option[(Array[Byte], Boolean)] = {
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
case class Included(val b: Array[Byte]) extends Bounded(b, true);

/**
 * 不包含边界值的边界
 *
 * @param b 边界值
 */
case class Excluded(val b: Array[Byte]) extends Bounded(b, false);
