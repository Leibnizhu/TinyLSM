package io.github.leibnizhu.tinylsm.compact

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}
import io.github.leibnizhu.tinylsm.{LsmStorageInner, LsmStorageState, SsTable}

@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME,
  include = JsonTypeInfo.As.PROPERTY,
  property = "type"
)
@JsonSubTypes(Array(
  new Type(value = classOf[SimpleCompactionTask], name = "simple"),
  new Type(value = classOf[FullCompactionTask], name = "full"),
  new Type(value = classOf[TieredCompactionTask], name = "tiered"),
  new Type(value = classOf[LeveledCompactionTask], name = "leveled"),
))
trait CompactionTask {
  /**
   * 执行compact操作，按需合并SST，生成新SST，但无需修改 LsmStorageInner 状态
   *
   * @param storage LsmStorageInner
   * @return compact后新的 SST
   */
  def doCompact(storage: LsmStorageInner): List[SsTable]

  /**
   * 应用 compact 结果修改新的LSM 状态 LsmStorageState。
   * 这个函数应该只更改 “l0SsTables” 和 “levels” ，而不更改各个 MemTable 和 “ssTables” 映射。
   * 虽然应该只有一个线程运行压缩作业，但您应该考虑当压缩器生成新的 SST 时 L0 SST 被刷新的情况
   *
   * @param state  LsmStorageState。调用该方法时已上了 stateLock 锁，请直接更新状态。不用处理ssTables
   * @param output 新的 sst ID 列表
   * @return 需要删除的 SST 文件的 SST ID
   */
  def applyCompactionResult(state: LsmStorageState, output: List[Int]): List[Int]

  /**
   * @return 当前任务是否 compact 到最底部的level，如果是的话，会删除 delete墓碑
   */
  def compactToBottomLevel(): Boolean
}
