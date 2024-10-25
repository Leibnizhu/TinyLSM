package io.github.leibnizhu.tinylsm.raft

import io.github.leibnizhu.tinylsm.raft.RaftRole.{Candidate, Follower, Leader}
import org.apache.pekko.actor.ActorSelection
import org.apache.pekko.actor.typed.scaladsl.AskPattern.*
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, AskPattern, Behaviors, TimerScheduler}
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.slf4j.LoggerFactory

import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue, ThreadLocalRandom}
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import scala.concurrent.duration.*


object RaftNode {
  private val logger = LoggerFactory.getLogger(this.getClass)

  //发送心跳间隔
  private val sendHeartbeatInterval = 200.millis
  //太长了会超过测试的时间限制（过了checkOneLeader的时间还没超时并重新选举），太短了会增加rpc总调用次数
  private val heartbeatTimeout = 2000.millis
  //选举超时的随机范围 从0ms到这个常量ms之间变化
  private val electionRange = 1000

  def apply(
             clusterName: String,
             nodes: Array[String],
             curIdx: Int,
             applyQueue: ArrayBlockingQueue[ApplyLogRequest],
             persistor: Persistor
           ): Behavior[Command] = Behaviors.withTimers { timers =>
    val initialState = RaftState(
      role = Follower,
      clusterName = clusterName,
      nodes = nodes,
      curIdx = curIdx,
      currentTerm = 0,
      votedFor = None,
      log = Array(),
      commitIndex = -1,
      lastApplied = -1,
      nextIndex = Array.fill(nodes.length)(0),
      matchIndex = Array.fill(nodes.length)(0),
      applyQueue = applyQueue,
      persistor = persistor,
      timers = timers,
    ).readPersist() // 恢复已持久化的状态
    raftBehavior(initialState)
  }

  /**
   * 定义处理状态和消息的行为
   * 所有角色：
   * 1. 如果commitIndex > lastApplied，那么就lastApplied加一，并把log[lastApplied]应用到状态机中（5.3节）
   * 2. 如果接收到的 RPC 请求或响应中，任期号T > currentTerm，那么就令currentTerm等于 T，并切换状态为跟随者（5.1节）
   */
  private def raftBehavior(state: RaftState): Behavior[Command] = state.role match {
    case Follower => follower(state)
    case Candidate => candidate(state)
    case Leader => leader(state)
  }


  /**
   * Follower（5.2节）：
   * 1. 响应来自Candidate和Leader的请求
   * 2. 如果在超过选举超时时间的情况之前没有收到当前Leader（即该Leader的任期需与这个Follower的当前任期相同）的心跳/附加日志，或者是给某个Candidate投了票，就自己变成Candidate
   */
  private def follower(state: RaftState): Behavior[Command] = Behaviors.setup { context =>
    if (logger.isDebugEnabled) {
      logger.debug("{}: START, hash={}", state.name(), state.hashCode())
    }
    state.timers.startSingleTimer(StartElection, StartElection, randomElectionTimeout)

    Behaviors.receive { (context, message) =>
      if (logger.isDebugEnabled) {
        logger.debug("{}: received: {}", state.name(), message)
      }
      message match {
        case appendLog: AppendLogRequest =>
          // 重置选举超时计时器
          val newState = handleAppendLogRequest(state, appendLog, context)
          // 这里会重新Behaviors.setup，设置StartElection定时器，所以不需要手动启动定时器
          raftBehavior(newState)

        case StartElection =>
          logger.info("{}: Election timeout, becoming Candidate", state.name())
          // 进入 Candidate 状态，增加任期
          raftBehavior(state.newCandidateElection().persist())

        case vote: VoteRequest =>
          val newState = handleVoteRequest(state, vote)
          if (newState.role != state.role || newState.currentTerm != state.currentTerm || newState.votedFor != state.votedFor) {
            raftBehavior(newState)
          } else {
            // 如果状态没有变化，则不创建新state，否则会重置timer，导致不会超时成为candidate
            Behaviors.same
          }

        // 状态查询
        case qs: QueryStateRequest => handleQueryState(state, qs)
        // 上层应用命令
        case command: CommandRequest => handleCommandRequestNotLeader(state, command)

        // 上层应用要求快照
        case snapshot: Snapshot => handleSnapshot(state, snapshot)
        // 按Leader要求安装快照
        case req: InstallSnapshotRequest => handleSnapshotInstallRequest(state, req, context)
        // 安装快照的响应
        case resp: InstallSnapshotResponse => handleInstallSnapshotResponse(state, resp)
        // 上层应用询问是否可以安装快照
        case req: CondInstallSnapshotRequest => handleCondInstallSnapshotRequest(state, req)

        case _: RenewState => raftBehavior(state)

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
  private def candidate(state: RaftState): Behavior[Command] = Behaviors.setup { context =>
    if (state.newElection) {
      logger.info("{}: Starting election for term {}", state.name(), state.currentTerm)
      state.timers.startSingleTimer(ElectionTimeout, ElectionTimeout, randomElectionTimeout)
      val voteReq = VoteRequest(state.currentTerm, state.curIdx, state.lastLogIndex(), state.lastLogTerm(), context.self)
      state.nodes.indices.filter(_ != state.curIdx).map(state.actorSelectionOf(context, _)).foreach(_ ! voteReq)
    }

    Behaviors.receive { (context, message) =>
      message match {
        case VoteResponse(term, voteGranted) =>
          if (term > state.currentTerm) {
            logger.info("{}: Received VoteResponse in term {}, stepping down to Follower", state.name(), term)
            state.timers.cancel(ElectionTimeout)
            raftBehavior(state.copy(role = Follower, votedFor = None, currentTerm = term).persist())
          } else {
            val newReceived = state.receivedVotes + 1
            val newGranted = state.grantedVotes + (if (voteGranted) 1 else 0)
            val nodeNum = state.nodes.length
            if (newGranted > nodeNum / 2) {
              // 足够票数，成为 Leader
              logger.info("{}: ==> Got {}/{}[{}] votes granted, win the election for Term{}, becoming ===>>>>[[[Leader]]]<<<===",
                state.name(), newGranted, newReceived, nodeNum, state.currentTerm)
              state.persist()
              state.timers.cancel(ElectionTimeout)
              // 成为leader前更新 nextIndex
              val nextIndex = state.lastLogIndex() + 1
              val newNextIndex = Array.fill(nodeNum)(nextIndex)
              logger.info("{}: Update nextIndex to: [{}]", state.name(), newNextIndex.mkString(","))
              // raft选举后假如当前term没有新start的entry，那么之前term遗留下的entry永远不会commit。这样会导致之前的请求一直等待，无法返回。所以每次raft选举后，发送一个消息，提醒server主动start一个新的entry
              state.applyNewLeader()
              raftBehavior(state.copy(role = Leader, nextIndex = newNextIndex))
            } else if (newReceived == nodeNum) {
              // 全部票收回，但未达到leader要求
              logger.info("{}: Got {}/{}[{}] votes granted, wait for election timeout", state.name(), newGranted, newReceived, nodeNum)
              //            Thread.sleep(ThreadLocalRandom.current().nextInt(electionRange))
              //              raftBehavior(state.newCandidateElection())
              Behaviors.same
            } else {
              logger.info("{}: Got {}/{}[{}] votes granted", state.name(), newGranted, newReceived, nodeNum)
              raftBehavior(state.copy(receivedVotes = newReceived, grantedVotes = newGranted, newElection = false))
            }
          }

        case ElectionTimeout =>
          logger.info("{}: Election timeout, start new election", state.name())
          raftBehavior(state.newCandidateElection())

        case appendLog: AppendLogRequest =>
          logger.info("{}: Received heartbeat from Leader Node{}, stepping down to Follower", state.name(), appendLog.leaderId)
          // 收到 Leader 的心跳，成为 Follower
          state.timers.cancel(ElectionTimeout)
          val newState = handleAppendLogRequest(state, appendLog, context).persist()
          raftBehavior(newState)

        case vote: VoteRequest =>
          if (vote.term > state.currentTerm) {
            state.timers.cancel(ElectionTimeout)
          }
          val newState = handleVoteRequest(state, vote)
          raftBehavior(newState.copy(newElection = false))

        // 状态查询
        case qs: QueryStateRequest => handleQueryState(state, qs)
        // 上层应用命令
        case c: CommandRequest => handleCommandRequestNotLeader(state, c)

        // 上层应用要求快照
        case snapshot: Snapshot => handleSnapshot(state, snapshot)
        // 按Leader要求安装快照
        case req: InstallSnapshotRequest => handleSnapshotInstallRequest(state, req, context)
        // 安装快照的响应
        case resp: InstallSnapshotResponse => handleInstallSnapshotResponse(state, resp)
        // 上层应用询问是否可以安装快照
        case req: CondInstallSnapshotRequest => handleCondInstallSnapshotRequest(state, req)

        // 忽略的消息
        case c: Command => handleUnsupportedMsg(state, c)
      }
    }
  }

  /**
   * Leader：
   * 1. 一旦成为Leader：发送空的附加日志 RPC（心跳）给其他所有的服务器；在一定的空余时间之后不停的重复发送，以阻止Follower超时（5.2节）
   * 2. 如果接收到来自上层应用的请求：附加条目到本地日志中，在条目被应用到状态机后响应上层应用（5.3节）
   * 3. 如果对于一个Follower，最后日志条目的索引值大于等于nextIndex，那么：发送从nextIndex开始的所有日志条目：
   * - a) 如果成功：更新相应Follower的nextIndex和matchIndex
   * - b) 如果因为日志不一致而失败，减少nextIndex重试
   * 4. 假设存在大于commitIndex的N，使得大多数的matchIndex[i] ≥ N成立，且log[N].term == currentTerm成立，则令commitIndex等于N（5.3和5.4节
   */
  private def leader(state: RaftState): Behavior[Command] = Behaviors.setup { context =>
    state.timers.startSingleTimer(SendHeartbeat, SendHeartbeat, sendHeartbeatInterval)

    Behaviors.receive { (context, message) =>
      message match {
        case SendHeartbeat =>
          for (i <- state.nodes.indices) {
            if (i != state.curIdx) {
              val request = generateAppendLogOrInstallSnapshotRequest(state, context, i)
              val ref = state.actorSelectionOf(context, i)
              if (logger.isDebugEnabled) {
                logger.debug(s"{} Create {} for Node{} {}", state.name(), request, i, ref)
              }
              ref ! request
            } else {
              // snapshot存储
              val curNextIndex = state.nextIndex(i)
              if (curNextIndex > 0 && curNextIndex - 1 < state.snapshotLastIndex) {
                state.applyCurrentStateSnapshot()
              }
            }
          }
          state.timers.startSingleTimer(SendHeartbeat, SendHeartbeat, sendHeartbeatInterval)
          Behaviors.same

        case CommandRequest(command, replyTo) =>
          val newLogIndex = state.lastLogIndex() + 1
          logger.info("{}: Received CommandRequest from client, appending command '{}' to log, new log index={}",
            state.name(), new String(command), newLogIndex)
          // 追加日志
          val newLog = state.log :+ LogEntry(state.currentTerm, newLogIndex, command)
          val newMatchIndex = state.matchIndex.clone()
          newMatchIndex(state.curIdx) = newLogIndex
          val newNextIndex = state.nextIndex.clone()
          newNextIndex(state.curIdx) = newLogIndex + 1
          val newState = state.copy(log = newLog, matchIndex = newMatchIndex, nextIndex = newNextIndex).persist()
          replyTo ! CommandResponse(newLogIndex, state.currentTerm, true)
          raftBehavior(newState)

        case logResp: AppendLogResponse =>
          val newState = handleAppendLogResponse(state, logResp, context)
          if (newState.role != Leader) {
            state.timers.cancel(SendHeartbeat)
          }
          raftBehavior(newState)


        case vote: VoteRequest =>
          val newState = handleVoteRequest(state, vote)
          if (newState.role != Leader) {
            state.timers.cancel(SendHeartbeat)
          }
          raftBehavior(newState)

        // 状态查询
        case qs: QueryStateRequest => handleQueryState(state, qs)

        // 上层应用要求快照
        case snapshot: Snapshot => handleSnapshot(state, snapshot)
        // 安装快照的响应
        case resp: InstallSnapshotResponse => handleInstallSnapshotResponse(state, resp)
        // 上层应用询问是否可以安装快照
        case req: CondInstallSnapshotRequest => handleCondInstallSnapshotRequest(state, req)

        // 忽略的信息
        case v: VoteResponse => handleNoLongerCandidate(state, v)
        case c: Command => handleUnsupportedMsg(state, c)
      }
    }
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
      logger.info("{}: REJECT vote because vote term {} < current term",
        state.name(), vote.term, state.currentTerm)
      vote.replyTo ! VoteResponse(state.currentTerm, false)
      return state.persist()
    }

    // 如果当前任期更小，需要更新
    val newState = if (vote.term > state.currentTerm) {
      logger.info("{}: Update term to {} according to Node{}'s VoteRequest {}",
        state.name(), vote.term, vote.candidateId, if (state.role != Follower) ", becoming Follower" else "")
      state.copy(role = Follower, currentTerm = vote.term, votedFor = None)
    } else state

    val lastLogTerm = newState.lastLogTerm()
    val lastLogIndex = newState.lastLogIndex()
    if (newState.votedFor.isDefined && newState.votedFor.get != vote.candidateId) {
      logger.info("{}: REJECT Node{}'s vote because has voted for Node{}",
        newState.name(), vote.candidateId, newState.votedFor.get)
      vote.replyTo ! VoteResponse(state.currentTerm, false)
      newState.persist()
    } else if (vote.lastLogTerm > lastLogTerm || (vote.lastLogTerm == lastLogTerm && vote.lastLogIndex >= lastLogIndex)) {
      // Candidate的日志至少和自己一样新
      vote.replyTo ! VoteResponse(newState.currentTerm, true)
      logger.info("{}: Voted to Node{} for term {}", newState.name(), vote.candidateId, vote.term)
      newState.copy(role = Follower, currentTerm = vote.term, votedFor = Some(vote.candidateId)).persist()
    } else {
      logger.info("{}: REJECT Node{}'s vote because current node's last log is {}@{}, candidate's last log is {}@{}",
        newState.name(), vote.candidateId, lastLogIndex, lastLogTerm, vote.lastLogIndex, vote.lastLogTerm)
      vote.replyTo ! VoteResponse(state.currentTerm, false)
      newState.persist()
    }
  }

  private def generateAppendLogOrInstallSnapshotRequest(state: RaftState, context: ActorContext[Command], i: Int): Command = {
    //当前要发送的节点的下一个同步日志索引(包含)
    val nodeNextIndex = state.nextIndex(i)
    val (prevLogIndex, prevLogTerm) = if (nodeNextIndex == 0) {
      (-1, -1)
    } else {
      val prevLogIndex = nodeNextIndex - 1
      if (prevLogIndex < state.snapshotLastIndex) {
        //        logger.info("{} Create InstallSnapshotRequest for Node{}, nodeNextIndex={}, snapshotLastIndex={}",
        //          state.name(), i, nodeNextIndex, state.snapshotLastIndex)
        // 需要发snapshot安装请求
        val installSnapshot = InstallSnapshotRequest(state.currentTerm, state.curIdx, state.snapshotLastIndex, state.snapshotLastTerm, state.snapshot, context.self)
        return installSnapshot
      } else if (prevLogIndex == state.snapshotLastIndex) {
        (prevLogIndex, state.snapshotLastTerm)
      } else {
        (prevLogIndex, state.getLogEntry(prevLogIndex).term)
      }
    }
    logger.debug("{}: Collecting log entries for Node{}, nextIndex={}, nodeNextIndex={}, prevLogIndex={}, prevLogTerm={}",
      state.name(), i, state.nextIndex.mkString(","), nodeNextIndex, prevLogIndex, prevLogTerm)
    val entries = if (state.log.nonEmpty && state.lastLogIndex() >= nodeNextIndex && state.firstLogIndex() <= nodeNextIndex) {
      val logLength = state.lastLogIndex() + 1 - nodeNextIndex
      state.log.slice(nodeNextIndex - state.firstLogIndex(), state.log.length)
    } else Array[LogEntry]()
    AppendLogRequest(state.currentTerm, state.curIdx, prevLogIndex, prevLogTerm, entries, state.commitIndex, context.self)
  }

  /**
   * AppendLogRequest 接收者的实现：
   * 1. 返回假如果领导者的任期小于接收者的当前任期（译者注：这里的接收者是指跟随者或者候选者）（5.1节）
   * 2. 返回假如果接收者日志中没有包含这样一个条目即该条目的任期在prevLogIndex上能和prevLogTerm匹配上（译者注：在接收者日志中如果能找到一个和prevLogIndex以及prevLogTerm一样的索引和任期的日志条目则继续执行下面的步骤否则返回假）（5.3节）
   * 3. 如果一个已经存在的条目和新条目（译者注：即刚刚接收到的日志条目）发生了冲突（因为索引相同，任期不同），那么就删除这个已经存在的条目以及它之后的所有条目（5.3节）
   * 4. 追加日志中尚未存在的任何新条目
   * 5. 如果领导者的已知已经提交的最高的日志条目的索引leaderCommit大于接收者的已知已经提交的最高的日志条目的索引commitIndex则把接收者的已知已经提交的最高的日志条目的索引commitIndex重置为领导者的已知已经提交的最高的日志条目的索引leaderCommit或者是上一个新条目的索引取两者的最小值
   */
  private def handleAppendLogRequest(state: RaftState, appendLog: AppendLogRequest, context: ActorContext[Command]): RaftState = {
    logger.debug("{}: Received AppendLogRequest, log: {}, prevLog: {}@Term{}",
      state.name(), appendLog.entries.toList, appendLog.prevLogIndex, appendLog.prevLogTerm)
    val curLastLogIndex = state.lastLogIndex()
    // 日志请求中的最大日志索引
    val maxLogIndex = if (appendLog.entries.isEmpty) -1 else appendLog.entries.last.index
    //任期判定
    if (appendLog.term < state.currentTerm) {
      logger.info("{}: REJECT AppendLogRequest because term {} is smaller than current node's Term {}",
        state.name(), appendLog.term, state.currentTerm)
      appendLog.replyTo ! AppendLogResponse(state.currentTerm, state.curIdx, maxLogIndex, false, curLastLogIndex)
      return state.persist()
    }

    // 如果leader term更大则清空当前投票
    val newVotedFor = if (appendLog.term > state.currentTerm) None else state.votedFor
    //如果当前没有包含 PrevLogIndex 和 PrevLogTerm 能匹配上的日志条目则返回false
    if (appendLog.prevLogIndex >= 0) {
      if (curLastLogIndex < appendLog.prevLogIndex) {
        logger.info("{}: REJECT AppendLogRequest because prevLogIndex {} is larger than last log index: {} {}, snapshotLastIndex:{}",
          state.name(), appendLog.prevLogIndex, curLastLogIndex, appendLog.entries, state.snapshotLastIndex)
        appendLog.replyTo ! AppendLogResponse(state.currentTerm, state.curIdx, maxLogIndex, false, curLastLogIndex + 1)
        return state.copy(role = Follower, votedFor = newVotedFor, currentTerm = appendLog.term).persist()
      }
      val rpcPrevLogEntry = state.getLogEntry(appendLog.prevLogIndex)
      if (rpcPrevLogEntry != null && rpcPrevLogEntry.term != appendLog.prevLogTerm) {
        logger.info("{}: REJECT AppendLogRequest because prevLogTerm={}, but current node's last log is in Term{}",
          state.name(), appendLog.prevLogTerm, rpcPrevLogEntry.term)
        val nextTryLogIndex = state.calNextTryLogIndex(appendLog, rpcPrevLogEntry)
        appendLog.replyTo ! AppendLogResponse(state.currentTerm, state.curIdx, maxLogIndex, false, nextTryLogIndex)
        return state.copy(role = Follower, votedFor = newVotedFor, currentTerm = appendLog.term).persist()
      }
    }

    //日志条目相关处理
    val newLog = ArrayBuffer[LogEntry]()
    newLog.appendAll(state.log)
    if (appendLog.entries != null) {
      for (rpcLogEntry <- appendLog.entries) {
        val existedEntry = state.getLogEntry(rpcLogEntry.index)
        if (existedEntry == null) {
          //rpc的日志在本地不存在，直接追加到本地日志
          newLog.append(rpcLogEntry)
        } else {
          //rpc的日志的索引在本地已存在
          //如果一个已经存在的条目和新条目发生了冲突（因为索引相同，任期不同），那么就删除这个已经存在的条目以及它之后的所有条目 （5.3 节
          if (existedEntry.term != rpcLogEntry.term) {
            //冲突的日志删除后，追加日志中尚未存在的任何新条目
            newLog.slice(0, rpcLogEntry.index - state.firstLogIndex())
            newLog.append(rpcLogEntry)
          }
          //rpc日志和现存日志相同的话，不用处理
        }
      }
    }

    //如果领导者的已知已经提交的最高的日志条目的索引leaderCommit 大于 接收者的已知已经提交的最高的日志条目的索引commitIndex
    //则把 接收者的已知已经提交的最高的日志条目的索引commitIndex 重置为
    //领导者的已知已经提交的最高的日志条目的索引leaderCommit 或者是 上一个新条目的索引 取两者的最小值
    val commitIndex = if (appendLog.leaderCommit > state.commitIndex) {
      Math.min(appendLog.leaderCommit, state.lastLogIndex())
    } else state.commitIndex

    // 应用命令
    val lastApplied = applyLogEntries(state, context, commitIndex)
    appendLog.replyTo ! AppendLogResponse(state.currentTerm, state.curIdx, maxLogIndex, true, 0)
    state.copy(role = Follower, votedFor = newVotedFor, currentTerm = appendLog.term,
      log = newLog.toArray, commitIndex = commitIndex, lastApplied = lastApplied).persist()
  }

  private def handleAppendLogResponse(state: RaftState, logResp: AppendLogResponse, context: ActorContext[Command]): RaftState = {
    // 按需更新任期
    if (logResp.term > state.currentTerm) {
      logger.info("{}: ==> Receive AppendLogResponse and get new Term{}, becoming Follower", state.name(), logResp.term)
      return state.copy(role = Follower, currentTerm = logResp.term, votedFor = None).persist()
    }

    val newMatchIndex = state.matchIndex.clone()
    val newNextIndex = state.nextIndex.clone()
    if (logResp.success) {
      if (logResp.maxLogIndex >= 0) {
        newMatchIndex(logResp.nodeIdx) = logResp.maxLogIndex
        newNextIndex(logResp.nodeIdx) = logResp.maxLogIndex + 1
      }
    } else {
      // Follower的日志与Leader的prevLogIndex以及prevLogTerm不匹配 如果因为日志不一致而失败，减少 nextIndex 重试
      // 当附加日志 RPC 的请求被拒绝的时候，Follower可以返回 冲突条目的任期号和该任期号对应的最小索引地址
      // 确定日志不匹配时的操作
      newNextIndex(logResp.nodeIdx) = Math.max(0, Math.min(logResp.nextTryLogIndex, state.lastLogIndex() - 1))
    }
    if (!state.matchIndex.sameElements(newMatchIndex) || !state.nextIndex.sameElements(newNextIndex)) {
      logger.info("{}: Receive AppendLogResponse {}, matchIndex: {} => {}, nextIndex: {} => {}",
        state.name(), logResp, state.matchIndex, newMatchIndex, state.nextIndex, newNextIndex)
    }

    // 假设存在大于 commitIndex 的 N，使得大多数的 matchIndex[i] ≥ N 成立，且 log[N].term == currentTerm 成立，则令 commitIndex 等于 N (§5.3, §5.4).
    var newCommitIndex = state.commitIndex
    var newLastApplied = state.lastApplied
    if (state.log.nonEmpty) {
      val baseLogIndex = state.firstLogIndex()
      val maybeN = (state.lastLogIndex() to Math.max(newCommitIndex, baseLogIndex) by -1)
        .filter(N => state.log(N - baseLogIndex).term == state.currentTerm)
        .find(N => newMatchIndex.count(_ >= N) >= state.nodes.length / 2 + 1)
      if (maybeN.isDefined) {
        newCommitIndex = maybeN.get
        // 应用命令
        newLastApplied = applyLogEntries(state, context, newCommitIndex)
      }
    }

    state.copy(matchIndex = newMatchIndex, nextIndex = newNextIndex, commitIndex = newCommitIndex, lastApplied = newLastApplied)
  }

  private def applyLogEntries(state: RaftState, context: ActorContext[Command], commitIndex: Int): Int = {
    var lastApplied = state.lastApplied
    if (commitIndex > lastApplied) {
      lastApplied += 1
      val entry = state.getLogEntry(lastApplied)
      if (entry != null) {
        state.applyLogEntry(entry)
        logger.info("{}: Applied 1 log, lastApplied={}", state.name(), lastApplied)
      }
    }
    lastApplied
  }

  private def handleSnapshot(state: RaftState, snapshot: Snapshot): Behavior[Command] =
    if (snapshot.index < state.snapshotLastIndex) {
      logger.info("{} is called Snapshot(), last index:{} is smaller than received snapshot index({}), skip handling...",
        state.name(), snapshot.index, state.snapshotLastIndex)
      Behaviors.same
    } else {
      //压缩日志
      val newLogFirstIndex = snapshot.index - state.firstLogIndex()
      //这里要先拿到当前日志里index对应的任期，否则修改snapshot的term/index后拿出来可能不对
      val term = state.getLogEntry(snapshot.index).term
      logger.info("{} is called Snapshot(), snapshot's last log {}@{}, start snapshot log",
        state.name(), snapshot.index, term)
      raftBehavior(state.copy(
        snapshot = snapshot.snapshot,
        snapshotLastIndex = snapshot.index,
        snapshotLastTerm = term,
        log = state.log.slice(newLogFirstIndex, state.log.length),
        newElection = false,
      ).persist())
    }

  private def handleSnapshotInstallRequest(state: RaftState, snapshotRequest: InstallSnapshotRequest,
                                           context: ActorContext[Command]): Behavior[Command] = {
    if (snapshotRequest.term < state.currentTerm) {
      //过期的请求
      logger.info("{} REJECT expired InstallSnapshotRequest from Leader Node{} with term={}",
        state.name(), snapshotRequest.leaderId, snapshotRequest.term)
      snapshotRequest.replyTo ! InstallSnapshotResponse(state.curIdx, state.currentTerm, snapshotRequest.term, state.snapshotLastIndex)
      return Behaviors.same
    }

    logger.info("{} Receive InstallSnapshotRequest from Leader Node{}: {}", state.name(), snapshotRequest.leaderId, snapshotRequest)
    if (snapshotRequest.lastIncludedIndex <= state.snapshotLastIndex) {
      //请求的快照更老
      logger.info("{} REJECT expired InstallSnapshotRequest, request's lastIncludedIndex {} <= current snapshotLastIndex {} {}",
        state.name(), snapshotRequest.lastIncludedIndex, state.snapshotLastIndex, state)
    } else {
      state.applyInstallSnapshotRequest(snapshotRequest)
    }
    snapshotRequest.replyTo ! InstallSnapshotResponse(state.curIdx, state.currentTerm, snapshotRequest.term, state.snapshotLastIndex)
    // leader的term可能更大，需要更新
    val newVotedFor = if (snapshotRequest.term > state.currentTerm) None else state.votedFor
    raftBehavior(state.copy(role = Follower, currentTerm = snapshotRequest.term, votedFor = newVotedFor).persist())
  }

  private def handleInstallSnapshotResponse(state: RaftState, resp: InstallSnapshotResponse) =
    if (resp.reqTerm != state.currentTerm || state.role != Leader) {
      Behaviors.same
    } else if (resp.term > state.currentTerm) {
      logger.info("{} send InstallSnapshotRequest is expired(remote node's term is {}), becoming Follower", state.name(), resp.term)
      raftBehavior(state.copy(role = Follower, currentTerm = resp.term, votedFor = None).persist())
    } else if (resp.lastIncludedIndex >= 0) {
      val newNextIndex = state.nextIndex.clone()
      newNextIndex(resp.nodeIdx) = resp.lastIncludedIndex + 1
      logger.info("{} node{} installed snapshot, nextIndex update to {}", state.name(), resp.nodeIdx, resp.lastIncludedIndex + 1)
      raftBehavior(state.copy(nextIndex = newNextIndex))
    } else {
      // 否则lastIncludedIndex<0 说明还没安装过snapshot
      Behaviors.same
    }

  private def handleCondInstallSnapshotRequest(state: RaftState, req: CondInstallSnapshotRequest) = {
    if (req.lastIncludedIndex < state.commitIndex) {
      logger.info("{} REJECT CondInstallSnapshotRequest because last log: {}@{} <= current commited index={}",
        state.name(), req.lastIncludedIndex, req.lastIncludedTerm, state.commitIndex)
      //快照较老，拒绝
      req.replyTo ! CondInstallSnapshotResponse(false)
      Behaviors.same
    } else {
      logger.info("{} Received CondInstallSnapshotRequest, last log: {}@{}, compress log end: {}, {}",
        state.name(), req.lastIncludedIndex, req.lastIncludedTerm, state.commitIndex, state.lastApplied)

      val newLog = if (req.lastIncludedIndex <= state.lastLogIndex() &&
        state.getLogEntry(req.lastIncludedIndex) != null &&
        state.getLogEntry(req.lastIncludedIndex).term == req.lastIncludedTerm) {
        //snapshot包含的日志比本地更旧，且对应的term能匹配上，则接受snapshot，压缩日志
        state.log.slice(req.lastIncludedIndex - state.snapshotLastIndex, state.log.length)
      } else {
        //否则放弃老日志（snapshot包含更新了的）
        Array[LogEntry]()
      }
      req.replyTo ! CondInstallSnapshotResponse(true)
      raftBehavior(state.copy(
        log = newLog,
        snapshotLastIndex = req.lastIncludedIndex,
        snapshotLastTerm = req.lastIncludedTerm,
        snapshot = req.snapshot,
        // IMPORTANT
        commitIndex = req.lastIncludedIndex,
        lastApplied = req.lastIncludedIndex,
      ).persist())
    }
  }

  private def randomElectionTimeout = {
    heartbeatTimeout + ThreadLocalRandom.current().nextInt(electionRange).millis
  }

  private def handleCommandRequestNotLeader(state: RaftState, c: CommandRequest): Behavior[Command] = {
    if (logger.isDebugEnabled) {
      logger.debug("{} Received CommandRequest {}", state.name(), c)
    }
    c.replyTo ! CommandResponse(-1, state.currentTerm, false)
    Behaviors.same
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
}
