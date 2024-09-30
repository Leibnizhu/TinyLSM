package io.github.leibnizhu.tinylsm.raft

import akka.actor.ActorSelection
import akka.actor.typed.scaladsl.{Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior}
import org.slf4j.LoggerFactory

import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.duration.*


object RaftNode {
  private val logger = LoggerFactory.getLogger(this.getClass)

  //发送心跳间隔
  private val sendHeartbeatInterval = 200.millis
  //太长了会超过测试的时间限制（过了checkOneLeader的时间还没超时并重新选举），太短了会增加rpc总调用次数
  private val heartbeatTimeout = 2000.millis
  //选举超时的随机范围 从0ms到这个常量ms之间变化
  private val electionRange = 700

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
    case Candidate => candidate(state, timers)
    case Leader => leader(state, timers)
  }


  /**
   * Follower（5.2节）：
   * 1. 响应来自Candidate和Leader的请求
   * 2. 如果在超过选举超时时间的情况之前没有收到当前Leader（即该Leader的任期需与这个Follower的当前任期相同）的心跳/附加日志，或者是给某个Candidate投了票，就自己变成Candidate
   */
  private def follower(state: RaftState, timers: TimerScheduler[Command]): Behavior[Command] = Behaviors.setup { context =>
    timers.startSingleTimer(StartElection, StartElection, randomElectionTimeout)

    Behaviors.receive { (context, message) =>
      message match {
        case appendLog: AppendLogRequest =>
          logger.debug("{}: Received heartbeat, resetting election timer", state.name())
          // 重置选举超时计时器
          timers.startSingleTimer(StartElection, StartElection, randomElectionTimeout)
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
          raftBehavior(state.newCandidateElection(), timers)

        case vote: VoteRequest =>
          val newState = handleVoteRequest(state, vote)
          raftBehavior(newState, timers)

        // 状态查询
        case qs: QueryStateRequest => handleQueryState(state, qs)

        // 忽略的信息
        case v: VoteResponse => handleNoLongerCandidate(state, v)
        case a: AppendLogResponse => handleNoLongerLeader(state, a)
        case c: Command => handleUnsupportedMsg(state, c)
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
    if (state.newElection) {
      logger.info("{}: Starting election for term {}", state.name(), state.currentTerm)
      timers.startSingleTimer(ElectionTimeout, ElectionTimeout, randomElectionTimeout)
      val voteReq = VoteRequest(state.currentTerm, state.curIdx, state.lastLogIndex(), state.lastLogTerm(), context.self)
      state.nodes.indices.filter(_ != state.curIdx).map(state.actorOf(context, _)).foreach(_ ! voteReq)
    }

    Behaviors.receive { (context, message) =>
      message match {
        case VoteResponse(term, voteGranted) =>
          val newReceived = state.receivedVotes + 1
          val newGranted = state.grantedVotes + (if (voteGranted) 1 else 0)
          if (newGranted > state.nodes.length / 2) {
            // 足够票数，成为 Leader
            logger.info("{}: ==> Got {}/{} votes granted, win the election for Term{}, becoming Leader",
              state.name(), newGranted, newReceived, state.currentTerm)
            timers.cancel(ElectionTimeout)
            raftBehavior(state.copy(role = Leader), timers)
          } else if (newReceived == state.nodes.length) {
            // 全部票收回，但未达到leader要求
            logger.info("{}: Got {}/{} votes granted, start new election", state.name(), newGranted, newReceived)
            //            Thread.sleep(ThreadLocalRandom.current().nextInt(electionRange))
            raftBehavior(state.newCandidateElection(), timers)
          } else {
            logger.info("{}: Got {}/{} votes granted", state.name(), newGranted, newReceived)
            raftBehavior(state.copy(receivedVotes = newReceived, grantedVotes = newGranted, newElection = false), timers)
          }

        case ElectionTimeout =>
          logger.info("{}: Election timeout, start new election", state.name())
          raftBehavior(state.newCandidateElection(), timers)

        case appendLog: AppendLogRequest =>
          logger.info("{}: Received heartbeat from Leader Node{}, stepping down to Follower", state.name(), appendLog.leaderId)
          // 收到 Leader 的心跳，成为 Follower
          timers.cancel(ElectionTimeout)
          raftBehavior(state.copy(role = Follower, votedFor = None), timers)

        case vote: VoteRequest =>
          if (vote.term > state.currentTerm) {
            timers.cancel(ElectionTimeout)
          }
          val newState = handleVoteRequest(state, vote)
          raftBehavior(newState.copy(newElection = false), timers)

        // 状态查询
        case qs: QueryStateRequest => handleQueryState(state, qs)

        // 忽略的消息
        case c: Command => handleUnsupportedMsg(state, c)
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
          logger.info("{}: Received client request, appending command ''{}'' to log", state.name(), command)
          // 追加日志
          val newLog = state.log :+ LogEntry(state.currentTerm, state.lastLogIndex() + 1, command)
          raftBehavior(state.copy(log = newLog), timers)

        case logResp: AppendLogResponse =>
          // TODO
          Behaviors.same

        case vote: VoteRequest =>
          val newState = handleVoteRequest(state, vote)
          if (newState.role != Leader) {
            timers.cancel(SendHeartbeat)
          }
          raftBehavior(newState, timers)

        // 状态查询
        case qs: QueryStateRequest => handleQueryState(state, qs)
        // 忽略的信息
        case v: VoteResponse => handleNoLongerCandidate(state, v)
        case c: Command => handleUnsupportedMsg(state, c)
      }
    }
  }

  private def randomElectionTimeout = {
    heartbeatTimeout + ThreadLocalRandom.current().nextInt(electionRange).millis
  }

  private def handleNoLongerCandidate(state: RaftState, c: Command): Behavior[Command] = {
    logger.info("{} is no longer a Candidate, ignore {}", state.name(), c.getClass.getSimpleName)
    Behaviors.same
  }

  private def handleNoLongerLeader(state: RaftState, c: Command): Behavior[Command] = {
    logger.info("{} is no longer a Leader, ignore {}", state.name(), c.getClass.getSimpleName)
    Behaviors.same
  }

  private def handleQueryState(state: RaftState, qs: QueryStateRequest): Behavior[Command] = {
    qs.replyTo ! QueryStateResponse(state)
    Behaviors.same
  }

  private def handleUnsupportedMsg(state: RaftState, c: Command): Behavior[Command] = {
    logger.warn("{} Unsupported message: {}", state.name(), c)
    Behaviors.same
  }

  /**
   * VoteRequest 接收者实现：
   * 1. 如果term < currentTerm返回false（5.2节）.
   * 2. 如果votedFor为空或者为candidateId，并且Candidate的日志至少和自己一样新，那么就投票给他（5.2节，5.4节
   */
  private def handleVoteRequest(state: RaftState, vote: VoteRequest): RaftState = {
    logger.info("{}: Received vote request from Node{}: {}, current voted: {}",
      state.name(), vote.candidateId, vote, state.votedFor)

    // candidate任期更小，否决选举
    if (vote.term < state.currentTerm) {
      logger.info("{}: reject vote because vote term {} < current term",
        state.name(), vote.term, state.currentTerm)
      vote.replyTo ! VoteResponse(state.currentTerm, false)
      return state
    }

    // 如果当前任期更小，需要更新
    val newState = if (vote.term > state.currentTerm) {
      logger.info("{}: Update term to {} according to Node{}''s VoteRequest {}",
        state.name(), vote.term, vote.candidateId, if (state.role != Follower) ", becoming Follower" else "")
      state.copy(role = Follower, currentTerm = vote.term, votedFor = None)
    } else state

    val lastLogTerm = newState.lastLogTerm()
    val lastLogIndex = newState.lastLogIndex()
    if (newState.votedFor.isDefined && newState.votedFor.get != vote.candidateId) {
      logger.info("{}: Reject Node{}''s vote because has voted for Node{}",
        newState.name(), vote.candidateId, newState.votedFor.get)
      vote.replyTo ! VoteResponse(state.currentTerm, false)
      newState
    } else if (vote.lastLogTerm > lastLogTerm || (vote.lastLogTerm == lastLogTerm && vote.lastLogIndex >= lastLogIndex)) {
      // Candidate的日志至少和自己一样新
      vote.replyTo ! VoteResponse(newState.currentTerm, true)
      logger.info("{}: Voted to Node{} for term {}", newState.name(), vote.candidateId, vote.term)
      newState.copy(role = Follower, currentTerm = vote.term, votedFor = Some(vote.candidateId))
    } else {
      logger.info("{}: Reject Node{}''s vote because current node''s last log is {}@{}, candidate''s last log is {}@{}",
        newState.name(), vote.candidateId, lastLogIndex, lastLogTerm, vote.lastLogIndex, vote.lastLogTerm)
      vote.replyTo ! VoteResponse(state.currentTerm, false)
      newState
    }
  }
}
