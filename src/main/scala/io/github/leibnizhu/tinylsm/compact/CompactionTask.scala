package io.github.leibnizhu.tinylsm.compact

import io.github.leibnizhu.tinylsm.{LsmStorageInner, SsTable}

trait CompactionTask {
  def doCompact(storage: LsmStorageInner): List[SsTable]

  def compactToBottomLevel(): Boolean
}
