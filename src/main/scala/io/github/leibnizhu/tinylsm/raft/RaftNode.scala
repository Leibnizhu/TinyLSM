package io.github.leibnizhu.tinylsm.raft

import akka.actor.ActorSelection
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior}
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.*
import scala.util.Random


sealed trait RaftRole {
  def shortName: String
}

case object Follower extends RaftRole:
  override def shortName = "F"

case object Candidate extends RaftRole:
  override def shortName = "C"

case object Leader extends RaftRole:
  override def shortName = "L"

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

case class LogEntry(term: Int, index: Int, command: String)

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
                      log: List[LogEntry],

                      //所有服务器上的易失性状态
                      // 已知已提交的最高的日志条目的索引（初始值为0，单调递增）
                      commitIndex: Int = 0,
                      // 已经被应用到状态机的最高的日志条目的索引（初始值为0，单调递增）
                      lastApplied: Int = 0,

                      // Candidate 易失状态,收到的投票响应和选中自己的
                      receivedVotes: AtomicInteger = AtomicInteger(0),
                      grantedVotes: AtomicInteger = AtomicInteger(0),

                      // Leader 上的易失性状态(选举后已经重新初始化)
                      // 对于每一台服务器，发送到该服务器的下一个日志条目的索引（初始值为领导者最后的日志条目的索引+1）
                      nextIndex: Array[Int],
                      // 对于每一台服务器，已知的已经复制到该服务器的最高日志条目的索引（初始值为0，单调递增）
                      matchIndex: Array[Int],
                    ) {
  def name(): String = s"[Raft ${role.shortName} Node$curIdx Term=$currentTerm]"

  def actorOf(context: ActorContext[Command], index: Int): ActorSelection =
    context.system.classicSystem.actorSelection(s"akka://$clusterName@${nodes(index)}/user")

  def nodeAddress(): String = nodes(curIdx)

  def lastLogTerm(): Int = {
    if (log.isEmpty) {
      //TODO snapshot判断
      -1
    } else {
      log.last.term
    }
  }

  def lastLogIndex(): Int = {
    if (log.isEmpty) {
      //TODO snapshot判断
      -1
    } else {
      log.last.index
    }
  }
}

object RaftNode {
  private val logger = LoggerFactory.getLogger(this.getClass)

  //发送心跳间隔
  private val sendHeartbeatInterval = 300.millis
  //太长了会超过测试的时间限制（过了checkOneLeader的时间还没超时并重新选举），太短了会增加rpc总调用次数
  private val heartbeatTimeout = 2000.millis
  //选举超时的随机范围 从0ms到这个常量ms之间变化
  private val electionRange = 500

  var log: List[LogEntry] = List()

  def apply(role: RaftRole, clusterName: String, nodes: Array[String], curIdx: Int): Behavior[Command] = Behaviors.withTimers { timers =>
    //TODO 恢复已持久化的状态
    val initialState = RaftState(
      role = Follower,
      clusterName = clusterName,
      nodes = nodes,
      curIdx = curIdx,
      currentTerm = 0,
      votedFor = None,
      log = List.empty[LogEntry],
      commitIndex = 0,
      lastApplied = 0,
      nextIndex = Array.fill(nodes.length)(0),
      matchIndex = Array.fill(nodes.length)(0),
    )
    raftBehavior(initialState, timers)
  }

  // 定义处理状态和消息的行为
  private def raftBehavior(state: RaftState, timers: TimerScheduler[Command]): Behavior[Command] = state.role match {
    case Follower => follower(state, timers)
    case Candidate => candidate(state.copy(
      votedFor = Some(state.curIdx),
      grantedVotes = new AtomicInteger(1),
      receivedVotes = new AtomicInteger(1)
    ), timers)
    case Leader => leader(state, timers)
  }


  /**
   * Follower（5.2节）：
   * 1. 响应来自Candidate和Leader的请求
   * 2. 如果在超过选举超时时间的情况之前没有收到当前Leader（即该Leader的任期需与这个Follower的当前任期相同）的心跳/附加日志，或者是给某个Candidate投了票，就自己变成Candidate
   */
  private def follower(state: RaftState, timers: TimerScheduler[Command]): Behavior[Command] = Behaviors.setup { context =>
    val random = new Random()
    timers.startSingleTimer(StartElection, StartElection, heartbeatTimeout + random.nextInt(electionRange).millis)

    Behaviors.receive { (context, message) =>
      message match {
        case appendLog: AppendLogRequest =>
          logger.debug("{}: Received heartbeat, resetting election timer", state.name())
          // 重置选举超时计时器
          timers.startSingleTimer(StartElection, StartElection, heartbeatTimeout + random.nextInt(electionRange).millis)
          //TODO 记录日志
          if (appendLog.term < state.currentTerm) {
            appendLog.replyTo ! AppendLogResponse(state.currentTerm, false)
            Behaviors.same
          } else {
            raftBehavior(state.copy(currentTerm = appendLog.term), timers)
          }

        case StartElection =>
          logger.info("{}: Election timeout, becoming Candidate", state.name())
          // 进入 Candidate 状态，增加任期
          raftBehavior(state.copy(role = Candidate, currentTerm = state.currentTerm + 1), timers)

        /**
         * 接收者实现：
         * 1. 如果term < currentTerm返回false（5.2节）.
         * 2. 如果votedFor为空或者为candidateId，并且Candidate的日志至少和自己一样新，那么就投票给他（5.2节，5.4节
         */
        case vote: VoteRequest =>
          logger.info("{}: Received vote request from Node{}: {}, current voted: {}", state.name(), vote.candidateId, vote, state.votedFor)
          timers.startSingleTimer(StartElection, StartElection, 2.seconds + random.nextInt(500).millis)
          if (vote.term < state.currentTerm) {
            logger.info("{}: reject vote because vote term {} < current term", state.name(), vote.term, state.currentTerm)
            vote.replyTo ! VoteResponse(state.currentTerm, false)
          }
          val lastLogTerm = state.lastLogTerm()
          val lastLogIndex = state.lastLogIndex()
          if ((state.votedFor.isEmpty || state.votedFor.get == vote.candidateId) &&
            (vote.lastLogTerm > lastLogTerm || (vote.lastLogTerm == lastLogTerm && vote.lastLogIndex >= lastLogIndex))) {
            vote.replyTo ! VoteResponse(state.currentTerm, true)
            logger.info("{}: voted to Candidate{} for term {}", state.name(), vote.candidateId, vote.term)
            raftBehavior(state.copy(role = Follower, currentTerm = vote.term, votedFor = Some(vote.candidateId)), timers)
          } else {
//            vote.replyTo ! VoteResponse(state.currentTerm, false)
            Behaviors.same
          }

        case c: Command =>
          logger.warn("{} Unsupported message: {}", state.name(), c)
          Behaviors.same
      }
    }
  }

  /**
   * Candidate（5.2节）：
   * 1. 在转变成Candidate后就立即开始选举过程
   * - a) 自增当前的任期号（currentTerm）
   * - b) 给自己投票
   * - c) 重置选举超时计时器
   * - d) 发送请求投票的 RPC 给其他所有服务器
   * 2. 如果接收到大多数服务器的选票，那么就变成Leader
   * 3. 如果接收到来自新的Leader的附加日志 RPC，转变成Follower
   * 4. 如果选举过程超时，再次发起一轮选举
   */
  private def candidate(state: RaftState, timers: TimerScheduler[Command]): Behavior[Command] = Behaviors.setup { context =>
    logger.info("{}: Starting election for term {}", state.name(), state.currentTerm)
    val random = new Random()
    timers.startSingleTimer(ElectionTimeout, ElectionTimeout, heartbeatTimeout + random.nextInt(electionRange).millis)
    val voteReq = VoteRequest(state.currentTerm, state.curIdx, state.lastLogIndex(), state.lastLogTerm(), context.self)
    state.nodes.indices.filter(_ != state.curIdx).map(state.actorOf(context, _)).foreach(_ ! voteReq)

    Behaviors.receive { (context, message) =>
      message match {
        case VoteResponse(term, voteGranted) =>
          val newReceived = state.receivedVotes.incrementAndGet()
          val newGranted = if (voteGranted) state.grantedVotes.incrementAndGet() else state.grantedVotes.get()
          if (newGranted > state.nodes.length / 2) {
            // 足够票数，成为 Leader
            logger.info("{}: Got {} vote response, totally got {} granted, becoming Leader", state.name(), newReceived, newGranted)
            timers.cancel(ElectionTimeout)
            raftBehavior(state.copy(role = Leader), timers)
          } else if (newReceived == state.nodes.length) {
            // 全部票收回，但未达到leader要求
            logger.info("{}: Got {} vote response, totally got {} granted, start new election", state.name(), newReceived, newGranted)
            raftBehavior(state.copy(role = Candidate, currentTerm = state.currentTerm + 1), timers)
          } else {
            logger.info("{}: Got {} vote response, totally got {} granted", state.name(), newReceived, newGranted)
            Behaviors.same
          }

        case ElectionTimeout =>
          logger.info("{}: election timeout, start new election", state.name())
          raftBehavior(state.copy(role = Candidate, currentTerm = state.currentTerm + 1), timers)

        case appendLog: AppendLogRequest =>
          logger.info("{}: Received heartbeat from Leader Node{}, stepping down to Follower", state.name(), appendLog.leaderId)
          // 收到 Leader 的心跳，成为 Follower
          timers.cancel(ElectionTimeout)
          raftBehavior(state.copy(role = Follower, votedFor = None), timers)

        case vote: VoteRequest =>
          if (vote.term > state.currentTerm) {
            vote.replyTo ! VoteResponse(state.currentTerm, true)
            timers.cancel(ElectionTimeout)
            raftBehavior(state.copy(role = Follower, currentTerm = vote.term), timers)
          } else {
            vote.replyTo ! VoteResponse(state.currentTerm, false)
            Behaviors.same
          }

        case c: Command =>
          logger.warn("{} Unsupported message: {}", state.name(), c)
          Behaviors.same
      }
    }
  }

  /**
   * Leader：
   * 1. 一旦成为Leader：发送空的附加日志 RPC（心跳）给其他所有的服务器；在一定的空余时间之后不停的重复发送，以阻止Follower超时（5.2节）
   * 2. 如果接收到来自客户端的请求：附加条目到本地日志中，在条目被应用到状态机后响应客户端（5.3节）
   * 3. 如果对于一个Follower，最后日志条目的索引值大于等于nextIndex，那么：发送从nextIndex开始的所有日志条目：
   * - a) 如果成功：更新相应Follower的nextIndex和matchIndex
   * - b) 如果因为日志不一致而失败，减少nextIndex重试
   * 4. 假设存在大于commitIndex的N，使得大多数的matchIndex[i] ≥ N成立，且log[N].term == currentTerm成立，则令commitIndex等于N（5.3和5.4节
   */
  private def leader(state: RaftState, timers: TimerScheduler[Command]): Behavior[Command] = Behaviors.setup { context =>
    timers.startSingleTimer(SendHeartbeat, SendHeartbeat, sendHeartbeatInterval)

    Behaviors.receive { (context, message) =>
      message match {
        case SendHeartbeat =>
          for (i <- state.nodes.indices) {
            if (i != state.curIdx) {
              val node = state.actorOf(context, i)
              //TODO 调整参数
              node ! AppendLogRequest(state.currentTerm, state.curIdx, 0, 0, List(), state.commitIndex, context.self)
            } else {
              //TODO snapshot存储
            }
          }
          timers.startSingleTimer(SendHeartbeat, SendHeartbeat, sendHeartbeatInterval)
          Behaviors.same

        case ClientRequest(command) =>
          logger.info("{}: Received client request, appending command '{}' to log", state.name(), command)
          // 追加日志
          val newLog = state.log :+ LogEntry(state.currentTerm, state.lastLogIndex() + 1, command)
          raftBehavior(state.copy(log = newLog), timers)

        case logResp: AppendLogResponse =>
          // TODO
          Behaviors.same

        case c: Command =>
          logger.warn("{} Unsupported message: {}", state.name(), c)
          Behaviors.same
      }
    }
  }

}
