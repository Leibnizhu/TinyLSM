package io.github.leibnizhu.tinylsm

import io.github.leibnizhu.tinylsm.compact.CompactionOptions
import io.github.leibnizhu.tinylsm.compact.CompactionOptions.*

import java.util
import java.util.concurrent.locks.{Lock, ReentrantLock, ReentrantReadWriteLock}

/**
 * 全是var，var里面是immutable的，需要snapshot的话直接copy这个state就可以
 * state.read(_.copy())
 */
case class LsmStorageState(
                            // 当前Memtable
                            var memTable: MemTable,
                            // 不可变的Memtable，从最近到最早的
                            var immutableMemTables: List[MemTable],
                            // L0 SST 从最新到最早的
                            var l0SsTables: List[Int],
                            // 按key 范围排序的SsTable， L1 -> Lmax，每层是 (L几，List(包含的SST ID))
                            var levels: List[Level] = List((1, List())),
                            // SST 对象
                            var ssTables: Map[Int, SsTable] = Map()
                          ) {
  // 对 MemTable 做 freeze 操作的读写锁
  private val (readLock, writeLock) = {
    val rwLock = ReentrantReadWriteLock()
    (rwLock.readLock(), rwLock.writeLock())
  }
  // MemTable 做 freeze 时，保证只有一个线程执行 freeze 的锁
  val stateLock: Lock = ReentrantLock()

  /**
   * 封装只读操作
   *
   * @param f 只读的操作
   * @tparam T 只读的响应类型
   * @return 只读的结果
   */
  def read[T](f: LsmStorageState => T): T = {
    try {
      readLock.lock()
      f(this)
    } finally {
      readLock.unlock()
    }
  }

  /**
   * 封装写操作
   *
   * @param f 写的操作
   * @tparam T 写操作的响应类型
   * @return 写的结果
   */
  def write[T](f: LsmStorageState => T): T = {
    try {
      writeLock.lock()
      f(this)
    } finally {
      writeLock.unlock()
    }
  }

  def dumpState(): String = {
    val snapshot = this.read(_.copy())
    val innerSb = new StringBuilder()
    innerSb.append(s"Current MemTable: ${snapshot.memTable.id}").append("\n")
    innerSb.append(s"Frozen MemTables: [${snapshot.immutableMemTables.map(_.id).mkString(", ")}]").append("\n")
    innerSb.append(s"L0\t(${snapshot.l0SsTables.length}): [${snapshot.l0SsTables.mkString(", ")}]").append("\n")
    for ((level, files) <- snapshot.levels) {
      innerSb.append(s"L$level\t(${files.length}): [${files.mkString(", ")}]").append("\n")
    }
    innerSb.append(s"SST: {${snapshot.ssTables.keys.mkString(", ")}}")
    println(innerSb.toString())
    innerSb.toString()
  }
}

object LsmStorageState {
  def apply(options: LsmStorageOptions): LsmStorageState = {
    val levels = options.compactionOptions match
      case SimpleCompactionOptions(_, _, maxLevels) => makeLevelsByMax(maxLevels)
      case LeveledCompactionOptions(_, _, maxLevels, _) => makeLevelsByMax(maxLevels)
      case FullCompaction | NoCompaction => makeLevelsByMax(1)
      case t: TieredCompactionOptions => List()
    new LsmStorageState(MemTable(0), List[MemTable](), List(), levels)
  }

  private def makeLevelsByMax(maxLevels: Int): List[Level] =
    (1 to maxLevels).map((_, List[Int]())).toList
}