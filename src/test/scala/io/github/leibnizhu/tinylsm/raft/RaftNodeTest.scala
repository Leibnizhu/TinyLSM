package io.github.leibnizhu.tinylsm.raft

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.pekko.actor.typed.scaladsl.AskPattern
import org.apache.pekko.actor.typed.scaladsl.AskPattern.*
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.LoggerFactory

import _root_.scala.runtime.stdLibPatches.Predef.assert

class RaftNodeTest extends AnyFunSuite {
  private val logger = LoggerFactory.getLogger(this.getClass)

  // 创建动态配置
  private def clusterConfigs(hosts: Array[String], clusterName: String): Array[Config] = {
    val seedNodesStr = hosts.map(h => s"\"pekko://${clusterName}@$h\"").mkString(",")
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

  private def startNodes(clusterName: String, configs: Array[Config]): Array[RaftNodeWrapper] = {
    configs.indices.toArray.map(i => {
      val wrapper = RaftNodeWrapper(clusterName, configs, i)
      wrapper.start()
      Thread.sleep(100)
      wrapper
    })
  }

  private def stopNodes(nodes: Array[RaftNodeWrapper]): Unit = {
    nodes.foreach(_.stop())
  }

  private def getLeader(nodes: Array[RaftNodeWrapper]): RaftNodeWrapper = {
    nodes.find(n => !n.stopped && n.getState.role == Leader)
      .getOrElse(throw new IllegalStateException("No Leader Found!!!"))

  }

  test("normal_3_nodes_election") {
    val clusterName = "TinyLsmCluster"
    val hosts = "localhost:2550,localhost:2551,localhost:2552".split(",")
    val configs = clusterConfigs(hosts, clusterName)

    // 启动所有节点，等待选举结束
    val nodeArr = startNodes(clusterName, configs)
    Thread.sleep(10000)

    val states = nodeArr.map(n => n.getState)
    val leaderCount = states.map(_.role).count(_ == Leader)
    val allTerms = states.map(_.currentTerm)
    for (state <- states) {
      println(state)
    }
    logger.info("Leader Count={}, all nodes' terms: {}", leaderCount, allTerms)
    assert(leaderCount == 1, "有且只能有一个Leader")
    assert(allTerms.forall(_ == allTerms.head), "所有人都是同一个Term")
    assert(allTerms.head < 20, "应该在20轮任期内完成Leader选举")
    stopNodes(nodeArr)
  }

  test("normal_5_nodes_election") {
    val clusterName = "TinyLsmCluster"
    val hosts = "localhost:2550,localhost:2551,localhost:2552,localhost:2553,localhost:2554".split(",")
    val configs = clusterConfigs(hosts, clusterName)

    // 启动所有节点，等待选举结束
    val nodeArr = startNodes(clusterName, configs)
    Thread.sleep(10000)

    val states = nodeArr.map(n => n.getState)
    val leaderCount = states.map(_.role).count(_ == Leader)
    val allTerms = states.map(_.currentTerm)
    for (state <- states) {
      println(state)
    }
    logger.info("Leader Count={}, all nodes' terms: {}", leaderCount, allTerms)
    assert(leaderCount == 1, "有且只能有一个Leader")
    assert(allTerms.forall(_ == allTerms.head), "所有人都是同一个Term")
    assert(allTerms.head < 20, "应该在20轮任期内完成Leader选举")
    stopNodes(nodeArr)
  }

  test("leader_stop_3_nodes_election") {
    val clusterName = "TinyLsmCluster"
    val hosts = "localhost:2550,localhost:2551,localhost:2552".split(",")
    val configs = clusterConfigs(hosts, clusterName)

    // 启动所有节点，等待选举结束
    val nodeArr = startNodes(clusterName, configs)
    Thread.sleep(5000)

    // 停止两次leader
    val leader1 = getLeader(nodeArr)
    val leaderName1 = leader1.getState.name()
    logger.info("==> Current Leader is {}, stopping", leaderName1)
    leader1.stop()
    Thread.sleep(5000)
    val leader2 = getLeader(nodeArr)
    val leaderName2 = leader2.getState.name()
    logger.info("==> Current Leader is {}, stopping", leaderName2)
    leader2.stop()
    Thread.sleep(5000)

    // 启动停止的节点
    logger.info("==> Node {} is starting", leaderName1)
    leader1.start()
    Thread.sleep(5000)
    logger.info("==> Node {} is starting", leaderName2)
    leader2.start()
    Thread.sleep(5000)

    val states = nodeArr.map(n => n.getState)
    val leaderCount = states.map(_.role).count(_ == Leader)
    val allTerms = states.map(_.currentTerm)
    for (state <- states) {
      println(state)
    }
    logger.info("Leader Count={}, all nodes' terms: {}", leaderCount, allTerms)
    assert(leaderCount == 1, "有且只能有一个Leader")
    assert(allTerms.forall(_ == allTerms.head), "所有人都是同一个Term")
    assert(allTerms.head < 50, "应该在50轮任期内完成3次Leader选举")
    stopNodes(nodeArr)
  }

  test("normal_3_nodes_append_log") {
    val clusterName = "TinyLsmCluster"
    val hosts = "localhost:2550,localhost:2551,localhost:2552".split(",")
    val configs = clusterConfigs(hosts, clusterName)

    // 启动所有节点，等待选举结束
    val nodeArr = startNodes(clusterName, configs)
    Thread.sleep(5000)
    val oldLeader = getLeader(nodeArr)
    oldLeader.system ! ClientRequest("ping".getBytes)
    Thread.sleep(3000)

    val oldLeaderState = oldLeader.getState
    logger.info("==> Current Leader is {}, matchIndex: {}, nextIndex: {}, stopping",
      oldLeaderState.name(), oldLeaderState.nextIndex.mkString(","), oldLeaderState.matchIndex.mkString(","))
    oldLeader.stop()
    Thread.sleep(3000)
    oldLeader.start()
    Thread.sleep(5000)

    val states = nodeArr.map(n => n.getState)
    for (state <- states) {
      println(state)
    }
    val newLeader = getLeader(nodeArr)
    val newLeaderState = newLeader.getState
    assert(oldLeaderState.matchIndex.sameElements(newLeaderState.matchIndex))
    assert(oldLeaderState.nextIndex.sameElements(newLeaderState.nextIndex))
    stopNodes(nodeArr)
  }


  test("leader_stop_and_recover_3_nodes_election") {
    val clusterName = "TinyLsmCluster"
    val hosts = "localhost:2550,localhost:2551,localhost:2552".split(",")
    val configs = clusterConfigs(hosts, clusterName)
    System.setProperty("raft.persistor", PersistorFactory.MEMORY)

    // 启动所有节点，等待选举结束
    val nodeArr = startNodes(clusterName, configs)
    Thread.sleep(5000)

    // 停止两次leader
    val leader1 = getLeader(nodeArr)
    val leaderState1 = leader1.getState
    val leaderPersistor1 = Some(leaderState1.persistor)
    logger.info("==> Current Leader is {}, stopping", leaderState1.name())
    leader1.stop()
    Thread.sleep(5000)
    val leader2 = getLeader(nodeArr)
    val leaderState2 = leader2.getState
    val leaderPersistor2 = Some(leaderState2.persistor)
    logger.info("==> Current Leader is {}, stopping", leaderState2.name())
    leader2.stop()
    Thread.sleep(5000)

    // 启动停止的节点
    logger.info("==> Node {} is starting", leaderState1.name())
    leader1.start(leaderPersistor1)
    Thread.sleep(5000)
    logger.info("==> Node {} is starting", leaderState2.name())
    leader2.start(leaderPersistor2)
    Thread.sleep(5000)

    val states = nodeArr.map(n => n.getState)
    val leaderCount = states.map(_.role).count(_ == Leader)
    val allTerms = states.map(_.currentTerm)
    for (state <- states) {
      println(state)
    }
    logger.info("Leader Count={}, all nodes' terms: {}", leaderCount, allTerms)
    assert(leaderCount == 1, "有且只能有一个Leader")
    assert(allTerms.forall(_ == allTerms.head), "所有人都是同一个Term")
    assert(allTerms.head < 50, "应该在50轮任期内完成3次Leader选举")
    // persist不为空
    assert(leaderPersistor1.get.readPersist().nonEmpty)
    assert(leaderPersistor2.get.readPersist().nonEmpty)
    stopNodes(nodeArr)
  }
}