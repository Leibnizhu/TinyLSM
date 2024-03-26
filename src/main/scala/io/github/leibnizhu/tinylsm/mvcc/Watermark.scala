package io.github.leibnizhu.tinylsm.mvcc

import java.util

case class Watermark(readers: util.TreeMap[Long, Int] = new util.TreeMap()) {

  def addReader(ts: Long): Unit = {

  }

  def removeReader(ts: Long): Unit = {

  }

  def watermark(): Option[Long] = {
    Some(0)
  }
}
