package io.github.leibnizhu.tinylsm.raft

import org.apache.pekko.actor.typed.scaladsl.AskPattern
import org.apache.pekko.actor.typed.scaladsl.AskPattern.*
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem, Scheduler}
import org.apache.pekko.util.Timeout
import org.slf4j.LoggerFactory

import _root_.scala.runtime.stdLibPatches.Predef.assert
import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration.*

case class RaftNodeWrapper(clusterName: String, hostsStr: String, curIdx: Int) extends RaftApp {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private var lastApplied = -1
  val logMap: mutable.Map[Int, Array[Byte]] = new mutable.HashMap()

  def getState: RaftState = askSelf[QueryStateResponse](ref => QueryStateRequest(ref)).state

  // 本地测试的时候，使用actorSelection可能获取不到其他 akka system 实例，发送的消息都会发送给自己，所以测试时传入所有 actor system
  var allSystem: Array[ActorSystem[Command]] = _

  override def stop(): Unit = {
    super.stop()
    allSystem = null
  }

  override def askOtherNode[Resp <: Command](nodeIdx: Int, makeReq: ActorRef[Resp] => ResponsibleCommand[Resp]): Resp = {
    if (allSystem != null && nodeIdx < allSystem.length && allSystem(nodeIdx) != null) {
      implicit val timeout: Timeout = 3.seconds
      implicit val scheduler: Scheduler = system.scheduler
      Await.result(allSystem(nodeIdx).ask[Resp](makeReq), 3.seconds)
    } else {
      super.askOtherNode(nodeIdx, makeReq)
    }
  }

  override def applyCommand(index: Int, command: Array[Byte]): Unit = {
    logger.info("Node{} is applying command(index={}): {}", curIdx, index, new String(command))
    if (index > lastApplied) {
      logMap(index) = command
      lastApplied = index
      if ((index + 1) % RaftNodeWrapper.snapShotInterval == 0) {
        // 模拟压缩日志
        snapshot(index, command)
      }
    }
  }

  override def applySnapshot(index: Int, term: Int, snapshot: Array[Byte]): Unit = {
    logger.info("Node{} is applying snapshot, last log {}@{}, snapshot length={}", curIdx, index, term, snapshot.length)
    val condInstall = askSelf[CondInstallSnapshotResponse](ref => CondInstallSnapshotRequest(term, index, snapshot, ref))
    if (condInstall.success) {
      // 用snapshot覆盖日志。由于压缩的时候省事，直接用最后command作为snapshot，所以这里snapshot可以直接作为压缩后的command放入日志
      logMap.clear()
      logMap(index) = snapshot
      lastApplied = index
    }
  }
}

object RaftNodeWrapper {
  val snapShotInterval = 10
}

