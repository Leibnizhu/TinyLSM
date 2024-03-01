package io.github.leibnizhu.tinylsm.compact

import io.github.leibnizhu.tinylsm.LsmStorageState
import io.github.leibnizhu.tinylsm.compact.CompactionOptions.*

case class CompactionController(option: CompactionOptions) {
  def generateCompactionTask(snapshot: LsmStorageState): Option[CompactionTask] = option match
    case NoCompaction => None
    case FullCompaction => {
      val l0SsTables = List(snapshot.l0SsTables: _*)
      val l1SsTables = if (snapshot.levels.isEmpty) List() else List(snapshot.levels.head._2: _*)
      Some(FullCompactionTask(l0SsTables, l1SsTables))
    }
    case simpleOption: SimpleCompactionOptions =>
      SimpleCompactionTask.generate(simpleOption, snapshot)
    case levelOption: LeveledCompactionOptions => None
    case tieredOption: TieredCompactionOptions => None
}
