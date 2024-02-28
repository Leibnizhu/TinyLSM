package io.github.leibnizhu.tinylsm.compact

import io.github.leibnizhu.tinylsm.{LsmStorageInner, SsTable}

case class LeveledCompactionTask() extends CompactionTask {
  override def doCompact(storage: LsmStorageInner): List[SsTable] = {
    // TODO
    null
  }

  override def compactToBottomLevel(): Boolean = true
}
