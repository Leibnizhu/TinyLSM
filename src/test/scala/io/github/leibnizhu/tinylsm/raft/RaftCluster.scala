package io.github.leibnizhu.tinylsm.raft

import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory

case class RaftCluster(clusterName: String, hosts: Array[String]) {
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

  val configs: Array[Config] = clusterConfigs(hosts, clusterName)
  var nodes: Array[RaftNodeWrapper] = _

  def startAll(): Unit = {
    nodes = configs.indices.toArray.map(i => {
      val wrapper = RaftNodeWrapper(clusterName, configs, i)
      wrapper.start()
      Thread.sleep(100)
      wrapper
    })
  }

  def stopAll(): Unit = {
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
}
