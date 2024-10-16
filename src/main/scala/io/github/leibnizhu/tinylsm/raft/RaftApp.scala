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


  private val logger = LoggerFactory.getLogger(this.getClass)
  private val hosts: Array[String] = hostsStr.split(",")
  private val applyQueue: BlockingQueue[ApplyLogRequest] = new ArrayBlockingQueue[ApplyLogRequest](1)
  private var threadPool: ExecutorService = _
  var system: ActorSystem[Command] = _
  private var lastLeader: Int = 0

  def start(persistorOption: Option[Persistor] = None): Unit = {
    val config = RaftApp.clusterConfig(hosts, curIdx, clusterName)
    val persistor = persistorOption.getOrElse(PersistorFactory.byConfig(curIdx))
    val raftNode = RaftNode(Follower, clusterName, hosts, curIdx, applyQueue, persistor)
    system = ActorSystem(raftNode, clusterName, config)
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

  def stop(): Unit = {
    system.terminate()
    Await.result(system.whenTerminated, Duration.Inf)
    system = null

    threadPool.shutdown()
    threadPool = null
  }

  def stopped: Boolean = system == null

  def askThisNode[Resp <: Command](makeReq: ActorRef[Resp] => ResponsibleCommand[Resp]): Resp = {
    implicit val timeout: Timeout = 3.seconds
    implicit val scheduler: Scheduler = system.scheduler
    Await.result(system.ask[Resp](makeReq), 3.seconds)
  }

  def askOtherNode[Resp <: Command](nodeIdx: Int, makeReq: ActorRef[Resp] => ResponsibleCommand[Resp]): Resp = {
    implicit val timeout: Timeout = 3.seconds
    implicit val scheduler: Scheduler = system.scheduler
    val actor = system.classicSystem.actorSelection(s"pekko://$clusterName@${hosts(nodeIdx)}/user")
    Await.result(system.ask[Resp](makeReq), 3.seconds)
  }

  def addCommand(command: Array[Byte]): Unit = {
    val startLeader = lastLeader
    var i = 0
    while (true) {
      val curTryLeader = (startLeader + i) % hosts.length
      try {
        val response: CommandResponse =
          if (curTryLeader == curIdx) askThisNode(ref => CommandRequest(command, ref)) else askOtherNode(curTryLeader, ref => CommandRequest(command, ref))
        if (response.isLeader) {
          logger.info("Node{} successfully add command at log {}@{}", curIdx, response.index, response.term)
          return
        } else {
          checkForSleep(i)
          i += 1
        }
      } catch
        case e: TimeoutException =>
          checkForSleep(i)
          i += 1
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

  def applyCommand(index: Int, command: Array[Byte]): Unit

  def applySnapshot(index: Int, term: Int, snapshot: Array[Byte]): Unit
}

object RaftApp {

  // 创建动态配置
  def clusterConfigs(hosts: Array[String], clusterName: String): Array[Config] = {
    val seedNodesStr = hosts.map(h => s"\"pekko://$clusterName@$h\"").mkString(",")
    hosts.map(h => {
      val hostAndPort = h.split(":")
      clusterConfig(seedNodesStr, hostAndPort(0), hostAndPort(1).toInt, clusterName)
    })
  }

  def clusterConfig(hosts: Array[String], curIdx: Int, clusterName: String): Config = {
    val seedNodesStr = hosts.map(h => s"\"pekko://$clusterName@$h\"").mkString(",")
    val hostAndPort = hosts(curIdx).split(":")
    clusterConfig(seedNodesStr, hostAndPort(0), hostAndPort(1).toInt, clusterName)
  }

  def clusterConfig(seedNodesStr: String, hostname: String, port: Int, clusterName: String): Config = {
    ConfigFactory.parseString(
      s"""
      pekko {
        actor {
          provider = "org.apache.pekko.remote.RemoteActorRefProvider"
          serializers {
            jackson-json = "org.apache.pekko.serialization.jackson.JacksonJsonSerializer"
          }
          serialization-bindings {
            "${classOf[Command].getName}" = jackson-json
          }
        }
        remote{
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
