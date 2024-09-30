package io.github.leibnizhu.tinylsm.raft

import akka.actor.typed.ActorRef


sealed trait Command extends Serializable

case object SendHeartbeat extends Command

case object StartElection extends Command

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
                             entries: List[LogEntry],
                             // Leader的已知已提交的最高的日志条目的索引
                             leaderCommit: Int,
                             replyTo: ActorRef[Command],
                           ) extends Command

case class AppendLogResponse(
                              //当前任期,对于领导者而言它会更新自己的任期
                              term: Int,
                              // 结果为真如果Follower所含有的条目和prevLogIndex以及prevLogTerm匹配上了
                              success: Boolean,
                            ) extends Command

case class ClientRequest(command: String) extends Command

case class QueryStateRequest(replyTo: ActorRef[Command]) extends Command

case class QueryStateResponse(state: RaftState) extends Command

case class LogEntry(term: Int, index: Int, command: String)