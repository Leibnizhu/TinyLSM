package io.github.leibnizhu.tinylsm.mvcc

import java.util
import java.util.concurrent.locks.ReentrantReadWriteLock

/**
 * 跟踪所有当前活动的 Transaction 的 readTs
 * 创建 Transaction 时调用 addReader 记录 Transaction 的 readTs
 * 当 Transaction 提交或中断时调用 removeReader 移除记录
 */
class Watermark {
  /**
   * 记录所有活动的 Transaction 的 Map[readTs => 使用数量]
   */
  private val readers: util.TreeMap[Long, Int] = new util.TreeMap()
  private val (readLock, writeLock) = {
    val rwLock = ReentrantReadWriteLock()
    (rwLock.readLock(), rwLock.writeLock())
  }

  /**
   * 创建 Transaction 时调用
   *
   * @param ts Transaction 的 readTs
   */
  def addReader(ts: Long): Unit = try {
    writeLock.lock()
    val cnt = readers.computeIfAbsent(ts, _ => 0) + 1
    readers.put(ts, cnt)
  } finally writeLock.unlock()

  /**
   * Transaction 提交或中断时调用
   *
   * @param ts Transaction 的 readTs
   */
  def removeReader(ts: Long): Unit = try {
    writeLock.lock()
    val cnt = readers.computeIfAbsent(ts, _ => 0) - 1
    if (cnt == 0) {
      readers.remove(ts)
    } else {
      readers.put(ts, cnt)
    }
  } finally writeLock.unlock()

  /**
   * 返回最小的 readTs，就是当前水位
   *
   * @return 如果没有正在进行的事务，返回 None
   */
  def watermark(): Option[Long] = try {
    readLock.lock()
    if (readers.isEmpty) None else Some(readers.firstKey())
  } finally readLock.unlock()

  def numRetainedSnapshots(): Int = try {
    readLock.lock()
    readers.size()
  } finally readLock.unlock()
}
