package io.github.leibnizhu.tinylsm.raft

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.pekko.actor.typed.Scheduler
import org.apache.pekko.actor.typed.scaladsl.AskPattern
import org.apache.pekko.actor.typed.scaladsl.AskPattern.*
import org.apache.pekko.util.Timeout
import org.slf4j.LoggerFactory

import scala.concurrent.duration.*
import scala.util.control.Breaks.*

case class RaftCluster(clusterName: String, hostNum: Int) {
  private val logger = LoggerFactory.getLogger(this.getClass)

  // 创建动态配置
  private def clusterConfigs(hosts: Array[String], clusterName: String): Array[Config] = {
    val seedNodesStr = hosts.map(h => s"\"pekko://$clusterName@$h\"").mkString(",")
    hosts.map(h => {
      val hostAndPort = h.split(":")
      clusterConfig(seedNodesStr, hostAndPort(0), hostAndPort(1).toInt, clusterName)
    })
  }

  private def clusterConfig(seedNodesStr: String, hostname: String, port: Int, clusterName: String): Config = {
    ConfigFactory.parseString(
      s"""
      pekko {
        actor {
          provider = "org.apache.pekko.remote.RemoteActorRefProvider"
          serializers {
            jackson-json = "org.apache.pekko.serialization.jackson.JacksonJsonSerializer"
          }
          serialization-bindings {
            "io.github.leibnizhu.tinylsm.raft.Command" = jackson-json
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

  val hosts: Array[String] = (0 until hostNum).map(i => s"localhost:${2550 + i}").toArray
  val configs: Array[Config] = clusterConfigs(hosts, clusterName)
  var nodes: Array[RaftNodeWrapper] = _

  def start(): Unit = {
    nodes = configs.indices.toArray.map(i => {
      val wrapper = RaftNodeWrapper(clusterName, configs, i)
      wrapper.start()
      Thread.sleep(100)
      wrapper
    })
  }

  def stop(): Unit = {
    nodes.foreach(_.stop())
  }

  def currentLeader(): (RaftNodeWrapper, Int) = {
    val states = nodes.filter(!_.stopped).map(_.getState)
    val leaderCount = states.map(_.role).count(_ == Leader)
    val allTerms = states.map(_.currentTerm)
    for (state <- states) {
      println(state)
    }
    logger.info("Leader Count={}, all nodes' terms: {}", leaderCount, allTerms)
    assert(leaderCount == 1, "有且只能有一个Leader")
    (nodes.find(n => !n.stopped && n.getState.role == Leader).get, allTerms.head)
  }

  def stopCurrentLeader(): (RaftNodeWrapper, RaftState) = {
    val (leader, term) = currentLeader()
    val leaderState = leader.getState
    logger.info("==> Current Leader is {}, stopping", leaderState.name())
    leader.stop()
    (leader, leaderState)
  }

  def sendOneCommand(cmd: Array[Byte], expectedServers: Int, retry: Boolean): Int = {
    val t0 = System.nanoTime()
    var starts = 0
    val timeout = 10.seconds
    val commandSent = false

    while (System.nanoTime() - t0 < timeout.toNanos) {
      // 尝试所有的服务器，可能有一个是领导者。
      var index = -1
      for (si <- hosts.indices) breakable {
        starts = (starts + 1) % hosts.length
        val rf = nodes(starts)
        if (rf != null && !rf.stopped) {
          implicit val timeout: Timeout = 3.seconds
          implicit val scheduler: Scheduler = rf.system.scheduler
          val response = rf.ask[CommandResponse](ref => CommandRequest(cmd, ref))
          if (response.isLeader) {
            index = response.index
            break
          }
        }
      }

      if (index != -1) {
        // 有人声称自己是领导者，并提交了我们的命令；等待一段时间以达成一致。
        val t1 = System.nanoTime()
        val agreementTimeout = 2.seconds

        var agreed = false
        while (System.nanoTime() - t1 < agreementTimeout.toNanos) {
          val (nd, cmd1) = nCommitted(index)
          if (nd > 0 && nd >= expectedServers) {
            // 已达成一致
            if (cmd1 sameElements cmd) {
              // 而且是我们提交的命令。
              return index
            }
          }
          Thread.sleep(20)
        }

        if (!commandSent) {
          throw new AssertionError(s"sendOneCommand(${new String(cmd)} failed to reach agreement")
        }
      } else {
        Thread.sleep(50)
      }

      throw new AssertionError(s"sendOneCommand(${new String(cmd)}  failed to reach agreement")
    }
    -1
  }

  def nCommitted(index: Int): (Int, Array[Byte]) = {
    var count = 0
    var cmd: Array[Byte] = null

    for (i <- nodes.indices) {
      val state = nodes(i).getState
      // 确保索引有效
      val cmd1 = if (index < state.log.length) state.log(index).command else null
      if (cmd1 != null) {

        if (count > 0 && (cmd != null && !(cmd sameElements cmd1))) {
          throw new RuntimeException(s"committed values do not match: index $index, ${new String(cmd)}, ${new String(cmd1)}")
        }
        count += 1
        cmd = cmd1
      }

    }
    (count, cmd) // 返回已提交的服务器数量和对应的命令
  }

}
