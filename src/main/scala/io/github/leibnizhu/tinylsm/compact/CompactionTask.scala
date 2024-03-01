package io.github.leibnizhu.tinylsm.compact

import io.github.leibnizhu.tinylsm.{LsmStorageInner, LsmStorageState, SsTable}

trait CompactionTask {
  /**
   * 执行compact操作，按需合并SST，生成新SST，但无需修改 LsmStorageInner 状态
   * @param storage LsmStorageInner
   * @return compact后新的 SST
   */
  def doCompact(storage: LsmStorageInner): List[SsTable]

  /**
   * 应用 compact 结果修改新的LSM 状态 LsmStorageState。
   * 这个函数应该只更改 “l0SsTables” 和 “levels” ，而不更改各个 MemTable 和 “ssTables” 映射。
   * 虽然应该只有一个线程运行压缩作业，但您应该考虑当压缩器生成新的 SST 时 L0 SST 被刷新的情况
   * @param state LsmStorageState
   * @param output 新的 sst ID 列表
   * @return 需要删除的 SST 文件的 SST ID
   */
  def applyCompactionResult(state: LsmStorageState, output: List[Int]): List[Int] = List()

  /**
   * @return 当前任务是否 compact 到最底部的level，如果是的话，会删除 delete墓碑
   */
  def compactToBottomLevel(): Boolean
}
