package io.github.leibnizhu.tinylsm.raft

import akka.actor.ActorSelection
import akka.actor.typed.scaladsl.ActorContext


// 定义状态数据结构
case class RaftState(
                      // 节点固定属性
                      clusterName: String,
                      nodes: Array[String],
                      curIdx: Int,

                      // 节点通用属性
                      role: RaftRole,

                      // 所有服务器上的持久性状态(在响应RPC请求之前已经更新到了稳定的存储设备
                      // 服务器已知最新的任期（在服务器首次启动的时候初始化为0，单调递增
                      currentTerm: Int,
                      // 当前任期内收到选票的候选者id如果没有投给任何候选者则为空
                      votedFor: Option[Int],
                      // 日志条目;每个条目包含了用于状态机的命令，以及领导者接收到该条目时的任期（第一个索引为1
                      log: Array[LogEntry],

                      //所有服务器上的易失性状态
                      // 已知已提交的最高的日志条目的索引（初始值为0，单调递增）
                      commitIndex: Int = -1,
                      // 已经被应用到状态机的最高的日志条目的索引（初始值为0，单调递增）
                      lastApplied: Int = -1,

                      // Candidate 易失状态,收到的投票响应和选中自己的
                      newElection: Boolean = false,
                      receivedVotes: Int = 0,
                      grantedVotes: Int = 0,

                      // Leader 的易失性状态(选举后已经重新初始化)
                      // 对于每一台服务器，发送到该服务器的下一个日志条目的索引（初始值为领导者最后的日志条目的索引+1）
                      nextIndex: Array[Int],
                      // 对于每一台服务器，已知的已经复制到该服务器的最高日志条目的索引（初始值为0，单调递增）
                      matchIndex: Array[Int],

                      // Snapshot相关
                      snapshot: Array[Byte] = Array(),
                      snapshotLastIndex: Int = -2,
                      snapshotLastTerm: Int = -2,
                    ) {
  def name(): String = s"[Raft ${role.shortName} Node$curIdx Term=$currentTerm]"

  override def toString: String = {
    val fields = productIterator.map {
      // 针对 Array 类型处理
      case array: Array[_] => array.mkString("[", ", ", "]")
      // 其他类型保持默认 toString
      case other => other.toString
    }.mkString(", ")

    s"${this.productPrefix}($fields)"
  }

  def actorOf(context: ActorContext[Command], index: Int): ActorSelection =
    context.system.classicSystem.actorSelection(s"akka://$clusterName@${nodes(index)}/user")

  def selfLogApplier(context: ActorContext[Command]): ActorSelection =
    context.system.classicSystem.actorSelection(s"akka://$clusterName@${nodes(curIdx)}/system/applyLog")

  def nodeAddress(): String = nodes(curIdx)

  def lastLogTerm(): Int =
    if (log.isEmpty) {
      //TODO snapshot判断
      -1
    } else {
      log.last.term
    }

  def lastLogIndex(): Int =
    if (log.isEmpty) {
      //TODO snapshot判断
      -1
    } else {
      log.last.index
    }

  def firstLogIndex(): Int =
    if (log.isEmpty) {
      -1
    } else {
      log.head.index
    }

  def getLogEntry(logIndex: Int): LogEntry =
    if (log.isEmpty) {
      if (logIndex == snapshotLastIndex) {
        LogEntry(snapshotLastTerm, logIndex, null)
      } else {
        null
      }
    } else {
      val firstIndex = firstLogIndex()
      if (logIndex < firstIndex || logIndex >= firstIndex + log.length) {
        null
      } else if (logIndex == snapshotLastIndex) {
        LogEntry(snapshotLastTerm, logIndex, null)
      } else {
        log(logIndex - firstIndex)
      }
    }

  /**
   * 计算冲突条目的任期号和该任期号对应的最小索引地址
   *
   * @param appendLog     AppendLogRequest请求
   * @param conflictEntry AppendLogRequest请求的最近日志条目
   * @return
   */
  def calNextTryLogIndex(appendLog: AppendLogRequest, conflictEntry: LogEntry): Int = {
    val term = conflictEntry.term
    val baseIndex = firstLogIndex()
    //遇到任期不相等的，返回下一个，下一个就是相等的任期
    (appendLog.prevLogIndex - 1 to baseIndex by -1).find(i => getLogEntry(i).term != term).map(_ + 1).getOrElse(-1)
  }

  def newCandidateElection(): RaftState = this.copy(
    role = Candidate,
    newElection = true,
    currentTerm = currentTerm + 1,
    votedFor = Some(curIdx),
    grantedVotes = 1,
    receivedVotes = 1)
}
