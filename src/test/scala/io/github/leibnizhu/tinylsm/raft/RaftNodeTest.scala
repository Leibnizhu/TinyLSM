package io.github.leibnizhu.tinylsm.raft

import akka.actor.typed.scaladsl.AskPattern
import akka.actor.typed.scaladsl.AskPattern.*
import akka.actor.typed.{ActorSystem, Scheduler}
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.funsuite.AnyFunSuite

import _root_.scala.concurrent.Await
import _root_.scala.runtime.stdLibPatches.Predef.assert
import scala.concurrent.duration.*

class RaftNodeTest extends AnyFunSuite {


  // 创建动态配置
  private def clusterConfigs(hosts: Array[String], clusterName: String): Array[Config] = {
    val seedNodesStr = hosts.map(h => s"\"akka://${clusterName}@$h\"").mkString(",")
    hosts.map(h => {
      val hostAndPort = h.split(":")
      clusterConfig(seedNodesStr, hostAndPort(0), hostAndPort(1).toInt, clusterName)
    })
  }

  private def clusterConfig(seedNodesStr: String, hostname: String, port: Int, clusterName: String): Config = {
    ConfigFactory.parseString(
      s"""
      akka {
        actor {
          provider = "akka.remote.RemoteActorRefProvider"
          serializers {
            jackson-json = "akka.serialization.jackson.JacksonJsonSerializer"
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
      Thread.sleep(100)
      wrapper
    })
  }

  test("normal_3_nodes_leader_election") {
    val clusterName = "TinyLsmCluster"
    val hosts = "localhost:2550,localhost:2551,localhost:2552".split(",")
    val configs = clusterConfigs(hosts, clusterName)

    // 启动所有节点，等待选举结束
    val nodeArr = startNodes(clusterName, configs)
    Thread.sleep(10000)

    val states = nodeArr.map(n => n.getState)
    val leaderCount = states.map(_.role).count(_ == Leader)
    for (state <- states) {
      println(state)
    }
    println(s"Leader Count=$leaderCount")
    assert(leaderCount == 1, "有且只能有一个Leader")
    val allTerms = states.map(_.currentTerm)
    assert(allTerms.forall(_ == allTerms.head), "所有人都是同一个Term")
    assert(allTerms.head < 20, "应该在20轮任期内完成Leader选举")
  }

  test("normal_5_nodes_leader_election") {
    val clusterName = "TinyLsmCluster"
    val hosts = "localhost:2550,localhost:2551,localhost:2552,localhost:2553,localhost:2554".split(",")
    val configs = clusterConfigs(hosts, clusterName)

    // 启动所有节点，等待选举结束
    val nodeArr = startNodes(clusterName, configs)
    Thread.sleep(10000)

    val states = nodeArr.map(n => n.getState)
    val leaderCount = states.map(_.role).count(_ == Leader)
    for (state <- states) {
      println(state)
    }
    println(s"Leader Count=$leaderCount")
    assert(leaderCount == 1, "有且只能有一个Leader")
    val allTerms = states.map(_.currentTerm)
    assert(allTerms.forall(_ == allTerms.head), "所有人都是同一个Term")
    assert(allTerms.head < 20, "应该在20轮任期内完成Leader选举")
  }
}
