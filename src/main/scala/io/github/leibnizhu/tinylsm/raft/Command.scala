package io.github.leibnizhu.tinylsm.raft

import org.apache.pekko.actor.typed.ActorRef


sealed trait Command extends Serializable

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
                        replyTo: ActorRef[Command],
                      ) extends Command

case class VoteResponse(
                         //当前任期号，以便于Candidate去更新自己的任期号
                         term: Int,
                         //Candidate赢得了此张选票时为真
                         voteGranted: Boolean) extends Command

case class LogEntry(term: Int, index: Int, command: Array[Byte])

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
                             replyTo: ActorRef[Command],
                           ) extends Command

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
 * 客户端的命令
 *
 * @param command 命令，对应日志记录的命令
 */
case class ClientRequest(command: Array[Byte]) extends Command

case class ApplyLogRequest(
                            commandValid: Boolean = false,
                            command: Array[Byte] = null,
                            commandIndex: Int = -1,
                            newLeader: Boolean = false) extends Command

case class QueryStateRequest(replyTo: ActorRef[Command]) extends Command

case class QueryStateResponse(state: RaftState) extends Command