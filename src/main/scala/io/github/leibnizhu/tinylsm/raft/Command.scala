package io.github.leibnizhu.tinylsm.raft

import org.apache.pekko.actor.typed.ActorRef


sealed trait Command extends Serializable

sealed trait ResponsibleCommand[Resp <: Command] extends Command {
  val replyTo: ActorRef[Resp]
}

/**
 * 让 Leader 发送心跳的事件
 */
case object SendHeartbeat extends Command

/**
 * Follower超时，让其开始变成Candidate发起选举的事件
 */
case object StartElection extends Command

/**
 * Candidate选举超时，让其重新发起选举的事件
 */
case object ElectionTimeout extends Command

case class VoteRequest(
                        // Candidate的任期号
                        term: Int,
                        //请求选票的Candidate的ID
                        candidateId: Int,
                        //Candidate的最后日志条目的索引值
                        lastLogIndex: Int,
                        //Candidate最后日志条目的任期号
                        lastLogTerm: Int,
                        replyTo: ActorRef[VoteResponse],
                      ) extends Command

case class VoteResponse(
                         //当前任期号，以便于Candidate去更新自己的任期号
                         term: Int,
                         //Candidate赢得了此张选票时为真
                         voteGranted: Boolean
                       ) extends Command

case class LogEntry(
                     term: Int,
                     index: Int,
                     command: Array[Byte]
                   ) {
  override def toString: String = s"LogEntry($index@$term, '${new String(command)}')"
}

case class AppendLogRequest(
                             //领导者的任期
                             term: Int,
                             // 领导者ID 因此Follower可以对客户端进行重定向 注：Follower根据领导者id把客户端的请求重定向到领导者，比如有时客户端把请求发给了Follower而不是领导者）
                             leaderId: Int,
                             // 紧邻新日志条目之前的那个日志条目的索引
                             prevLogIndex: Int,
                             // 紧邻新日志条目之前的那个日志条目的任期
                             prevLogTerm: Int,
                             // 需要被保存的日志条目（被当做心跳使用时，则日志条目内容为空；为了提高效率可能一次性发送多个）
                             entries: Array[LogEntry],
                             // Leader的已知已提交的最高的日志条目的索引
                             leaderCommit: Int,
                             replyTo: ActorRef[AppendLogResponse],
                           ) extends ResponsibleCommand[AppendLogResponse]

case class AppendLogResponse(
                              //当前任期,对于领导者而言它会更新自己的任期
                              term: Int,
                              // 发出响应的节点下标
                              nodeIdx: Int,
                              // AppendLogRequest里面最后一条日志的下标。 -1 表示没有日志
                              maxLogIndex: Int,
                              // 结果为真如果Follower所含有的条目和prevLogIndex以及prevLogTerm匹配上了
                              success: Boolean,
                              //当附加日志 RPC 的请求被拒绝的时候，跟随者可以(返回)冲突条目的任期号和该任期号对应的最小索引地址
                              nextTryLogIndex: Int,
                            ) extends Command

/**
 * 【上层应用发送】 的命令（日志操作）
 *
 * @param command 命令，对应日志记录的命令
 * @param replyTo 响应给上层应用用
 */
case class CommandRequest(
                           command: Array[Byte],
                           replyTo: ActorRef[CommandResponse]
                         ) extends ResponsibleCommand[CommandResponse]

case class CommandResponse(
                            index: Int,
                            term: Int,
                            isLeader: Boolean
                          ) extends Command

/**
 * 发送给上层应用，让上层应用应用日志的命令
 *
 * @param commandValid 命令是否可用
 * @param command      命令，对应日志记录的命令
 * @param commandIndex 命令索引
 * @param newLeader    是否产生了新leader
 */
case class ApplyLogRequest(
                            // 正常应用命令
                            commandValid: Boolean = false,
                            command: Array[Byte] = null,
                            commandIndex: Int = -1,

                            // snapshot相关
                            snapshotValid: Boolean = false,
                            snapshot: Array[Byte] = null,
                            snapshotTerm: Int = -1,
                            snapshotIndex: Int = -1,

                            // 产生了新leader
                            newLeader: Boolean = false
                          ) extends Command {
  override def toString: String = if (commandValid) {
    s"ApplyLogRequest: Command($commandIndex, ${new String(command)})"
  } else if (snapshotValid) {
    s"ApplyLogRequest: Snapshot($snapshotIndex@$snapshotTerm@, snapshot size: ${snapshot.length})"
  } else if (newLeader) {
    s"ApplyLogRequest: NewLeader()"
  } else {
    super.toString
  }
}

object ApplyLogRequest {
  def newLeader(): ApplyLogRequest = ApplyLogRequest(newLeader = true)

  def logEntry(logEntry: LogEntry): ApplyLogRequest = ApplyLogRequest(
    commandValid = true,
    command = logEntry.command,
    commandIndex = logEntry.index
  )

  def snapshot(snapshotRequest: InstallSnapshotRequest): ApplyLogRequest = ApplyLogRequest(
    snapshotValid = true,
    snapshot = snapshotRequest.data,
    snapshotTerm = snapshotRequest.lastIncludedTerm,
    snapshotIndex = snapshotRequest.lastIncludedIndex
  )
}

case class QueryStateRequest(
                              replyTo: ActorRef[QueryStateResponse]
                            ) extends ResponsibleCommand[QueryStateResponse]

case class QueryStateResponse(state: RaftState) extends Command

/**
 * 【上层应用发送】 的snapshot请求
 *
 * @param index    快照包含的最大日志index
 * @param snapshot 快照内容
 */
case class Snapshot(index: Int, snapshot: Array[Byte]) extends Command

/**
 * Raft Leader 节点告诉滞后的 Follower 节点用快照替换其状态
 *
 * @param term              term
 * @param leaderId          leader序号
 * @param lastIncludedIndex 快照包含的最大index
 * @param lastIncludedTerm  快照包含的最大term
 * @param data              快照本照
 * @param replyTo           leader地址
 */
case class InstallSnapshotRequest(
                                   term: Int,
                                   leaderId: Int,
                                   lastIncludedIndex: Int,
                                   lastIncludedTerm: Int,
                                   data: Array[Byte],
                                   replyTo: ActorRef[InstallSnapshotResponse]
                                 ) extends Command

/**
 * 安装快照的响应
 *
 * @param nodeIdx           当前节点下标
 * @param term              当前节点的Term
 * @param reqTerm           InstallSnapshotRequest请求带过来的Term
 * @param lastIncludedIndex 安装的快照的lastIncludedIndex
 */
case class InstallSnapshotResponse(
                                    nodeIdx: Int,
                                    term: Int,
                                    reqTerm: Int,
                                    lastIncludedIndex: Int
                                  ) extends Command

/**
 * 【上层应用】收到快照的ApplyLogRequest后，调用这个判断是否快照是否最新、是否可以安装快照
 *
 * @param lastIncludedTerm  快照包含的最大日志term
 * @param lastIncludedIndex 快照包含的最大日志index
 * @param snapshot          快照内容
 * @param replyTo           响应给上层应用用
 */
case class CondInstallSnapshotRequest(
                                       lastIncludedTerm: Int,
                                       lastIncludedIndex: Int,
                                       snapshot: Array[Byte],
                                       replyTo: ActorRef[CondInstallSnapshotResponse],
                                     ) extends ResponsibleCommand[CondInstallSnapshotResponse]

case class CondInstallSnapshotResponse(success: Boolean) extends Command

case class RenewState() extends Command