package io.github.leibnizhu.tinylsm.compact

import io.github.leibnizhu.tinylsm.compact.CompactionOptions.*
import io.github.leibnizhu.tinylsm.{LsmStorageState, SsTable}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class CompactionController(option: CompactionOptions) {
  def generateCompactionTask(snapshot: LsmStorageState): Option[CompactionTask] = option match
    case NoCompaction => None
    case FullCompaction =>
      val l0SsTables = List(snapshot.l0SsTables: _*)
      val l1SsTables = if (snapshot.levels.isEmpty) List() else List(snapshot.levels.head._2: _*)
      Some(FullCompactionTask(l0SsTables, l1SsTables))
    case simpleOption: SimpleCompactionOptions =>
      SimpleCompactionTask.generate(simpleOption, snapshot)
    case leveledOption: LeveledCompactionOptions =>
      LeveledCompactionTask.generate(leveledOption, snapshot)
    case tieredOption: TieredCompactionOptions =>
      TieredCompactionTask.generate(tieredOption, snapshot)

  /**
   * @return flush Memtable 时是否flush到L0，true则是，false则flush到levels
   */
  def flushToL0(): Boolean = !option.isInstanceOf[TieredCompactionOptions]

  def applyCompactionToState(state: LsmStorageState, newSsTables: List[SsTable], task: CompactionTask): List[Int] = {
    val newSstIds = newSsTables.map(_.sstId())
    val sstToRemove = new ListBuffer[Int]()
    try {
      state.stateLock.lock()
      val snapshot = state.read(_.copy())
      state.ssTables = state.ssTables ++ newSsTables.map(sst => sst.sstId() -> sst)
      val sstFileToRemove = task.applyCompactionResult(state, newSstIds)
      sstFileToRemove.foreach(sstId => {
        if (state.ssTables.contains(sstId)) {
          state.ssTables = state.ssTables - sstId
          sstToRemove += sstId
        }
      })
      sstToRemove.toList
    } finally {
      state.stateLock.unlock()
    }
  }
}
