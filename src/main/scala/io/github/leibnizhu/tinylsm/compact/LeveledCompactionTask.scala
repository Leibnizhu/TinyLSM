package io.github.leibnizhu.tinylsm.compact

import io.github.leibnizhu.tinylsm.{LsmStorageInner, LsmStorageState, SsTable}
import org.slf4j.LoggerFactory

case class LeveledCompactionTask() extends CompactionTask {
  override def doCompact(storage: LsmStorageInner): List[SsTable] = {
    // TODO
    null
  }

  override def applyCompactionResult(state: LsmStorageState, output: List[Int]): List[Int] = ???

  override def compactToBottomLevel(): Boolean = true
}

object LeveledCompactionTask {
  private val log = LoggerFactory.getLogger(classOf[LeveledCompactionTask])

  /**
   *
   * @param options  Compaction配置
   * @param snapshot Lsm内部状态 snapshot
   * @return 压缩任务。如果无需Compaction则返回None
   */
  def generate(options: CompactionOptions.LeveledCompactionOptions, snapshot: LsmStorageState): Option[LeveledCompactionTask] = {

    None
  }

}