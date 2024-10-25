package io.github.leibnizhu.tinylsm.raft

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.pekko.actor.typed.scaladsl.AskPattern
import org.apache.pekko.actor.typed.scaladsl.AskPattern.*
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem, Scheduler}
import org.apache.pekko.util.Timeout
import org.slf4j.LoggerFactory

import java.util.concurrent.*
import scala.concurrent.Await
import scala.concurrent.duration.*

trait RaftApp {
  val clusterName: String
  val hostsStr: String
  val curIdx: Int

  private val logger = LoggerFactory.getLogger(classOf[RaftApp])
  private val hosts: Array[String] = hostsStr.split(",")
  private val applyQueue: ArrayBlockingQueue[ApplyLogRequest] = new ArrayBlockingQueue[ApplyLogRequest](1)
  private var threadPool: ExecutorService = _
  var system: ActorSystem[Command] = _
  private var lastLeader: Int = 0

  def start(persistorOption: Option[Persistor] = None): Unit = synchronized {
    val config = RaftApp.clusterConfig(hosts, curIdx)
    val persistor = persistorOption.getOrElse(PersistorFactory.byConfig(curIdx))
    val raftNode = RaftNode(clusterName, hosts, curIdx, applyQueue, persistor)
    system = ActorSystem(raftNode, s"$clusterName-$curIdx", config)
    threadPool = Executors.newFixedThreadPool(1)
    threadPool.submit(new Runnable {
      override def run(): Unit = {
        while (!Thread.currentThread().isInterrupted) {
          val ap = applyQueue.take()
          if (ap.newLeader) {
            logger.info("Node{} received ApplyLogRequest: becoming Leader", curIdx)
          } else if (ap.commandValid) {
            applyCommand(ap.commandIndex, ap.command)
          } else if (ap.snapshotValid) {
            applySnapshot(ap.snapshotIndex, ap.snapshotTerm, ap.snapshot)
          } else {
            logger.info("Node{} Applying Log: {}", curIdx, ap)
          }
        }
        logger.info("Node{} is interrupted, exiting...", curIdx)
      }
    })
  }

  def stop(): Unit = synchronized {
    system.terminate()
    Await.result(system.whenTerminated, Duration.Inf)
    system = null

    threadPool.shutdown()
    threadPool = null
  }

  def stopped: Boolean = system == null || threadPool == null

  def ask[Resp <: Command](nodeIdx: Int, makeReq: ActorRef[Resp] => ResponsibleCommand[Resp]): Resp =
    if (nodeIdx == curIdx) askSelf(makeReq) else askOtherNode(nodeIdx, makeReq)

  def askSelf[Resp <: Command](makeReq: ActorRef[Resp] => ResponsibleCommand[Resp]): Resp = {
    implicit val timeout: Timeout = 3.seconds
    implicit val scheduler: Scheduler = system.scheduler
    Await.result(system.ask[Resp](makeReq), 3.seconds)
  }

  def askOtherNode[Resp <: Command](nodeIdx: Int, makeReq: ActorRef[Resp] => ResponsibleCommand[Resp]): Resp = {
    implicit val timeout: Timeout = 3.seconds
    implicit val scheduler: Scheduler = system.scheduler
    val actorSelection = system.classicSystem.actorSelection(s"pekko://$clusterName-$nodeIdx@${hosts(nodeIdx)}/user")
    val actor = Await.result(actorSelection.resolveOne(2.seconds), 2.seconds)
    logger.warn("===> Node{} resolved actor for Node{} got: {}", curIdx, nodeIdx, actor)
    import org.apache.pekko.pattern.ask
    try {
      // FIXME 应该是类似 typed Actor的ask模式，做一个临时的actor接收消息，而不是用当前的ActorSystem接收消息
      //      actorSelection ? makeReq(system)
      Await.result(actorSelection.ask(makeReq(system)).asInstanceOf[scala.concurrent.Future[Resp]], 3.seconds)
      //      Await.result(actor.ask(makeReq(null)).asInstanceOf[scala.concurrent.Future[Resp]], 3.seconds)
    } catch
      case e: TimeoutException =>
        logger.error("timeout")
        throw e
  }

  def tellOtherNode(nodeIdx: Int, req: Command): Unit = {
    val actorSelection = system.classicSystem.actorSelection(s"pekko://$clusterName-$nodeIdx@${hosts(nodeIdx)}/user")
    actorSelection ! req
  }

  def tellSelf(req: Command): Unit = {
    system.tell(req)
  }

  def addCommand(command: Array[Byte]): Unit = {
    val startLeader = lastLeader
    var i = 0
    while (true) {
      val curTryLeader = (startLeader + i) % hosts.length
      try {
        val response: CommandResponse = if (curTryLeader == curIdx) askSelf(ref => CommandRequest(command, ref)) else askOtherNode(curTryLeader, ref => CommandRequest(command, ref))
        //        val response: CommandResponse = askOtherNode(curTryLeader, ref => CommandRequest(command, ref))
        if (response.isLeader) {
          lastLeader = curTryLeader
          logger.info("Current Node{} => Node{} successfully add command at log {}@{}", curIdx, curTryLeader, response.index, response.term)
          return
        } else {
          logger.info("Current Node{} => Node{} response: {}. startLeader={},i={}", curIdx, curTryLeader, response, startLeader, i)
          checkForSleep(i)
          i += 1
        }
      } catch
        case e: TimeoutException =>
          logger.info("Current Node{} => Node{} timeout: {}. startLeader={},i={}", curIdx, curTryLeader, e, startLeader, i)
          checkForSleep(i)
          i += 1
        case e: Exception =>
          throw e
    }
  }

  private def checkForSleep(i: Int): Unit = {
    if ((i + 1) % hosts.length == 0) {
      logger.warn("Node{} traversed all nodes but can't find a Leader", curIdx)
      Thread.sleep(1000)
    } else {
      Thread.sleep(100)
    }
  }

  def snapshot(index: Int, snapshot: Array[Byte]): Unit = tellSelf(Snapshot(index, snapshot))

  def applyCommand(index: Int, command: Array[Byte]): Unit

  def applySnapshot(index: Int, term: Int, snapshot: Array[Byte]): Unit
}

object RaftApp {

  // 创建动态配置
  def clusterConfigs(hosts: Array[String]): Array[Config] = {
    hosts.map(h => {
      val hostAndPort = h.split(":")
      clusterConfig(hostAndPort(0), hostAndPort(1).toInt)
    })
  }

  def clusterConfig(hosts: Array[String], curIdx: Int): Config = {
    val hostAndPort = hosts(curIdx).split(":")
    clusterConfig(hostAndPort(0), hostAndPort(1).toInt)
  }

  def clusterConfig(hostname: String, port: Int): Config = {
    ConfigFactory.parseString(
      s"""
      pekko {
        actor {
          provider = "remote"
          serializers {
            jackson-json = "org.apache.pekko.serialization.jackson.JacksonJsonSerializer"
          }
          serialization-bindings {
            "${classOf[Command].getName}" = jackson-json
          }
        }
        remote {
          artery {
            enabled = on
            transport = tcp
            canonical.hostname = "$hostname"
            canonical.port = $port
          }
          warn-about-direct-use = false
          use-unsafe-remote-features-outside-cluster = true
        }
      }
    """).withFallback(ConfigFactory.load())
  }
}
