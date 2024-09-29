package io.github.leibnizhu.tinylsm.raft


import akka.actor.typed.ActorSystem
import com.typesafe.config.{Config, ConfigFactory}

object TestEntry {

  // 创建动态配置
  private def clusterConfigs(hosts: Array[String], clusterName: String): Array[Config] = {
    val seedNodesStr = hosts.map(h => s"\"akka://${clusterName}@${h}\"").mkString(",")
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

  def main(args: Array[String]): Unit = {
    val clusterName = "TinyLsmCluster"
    val hosts = "localhost:2550,localhost:2551,localhost:2552".split(",")
    val configs = clusterConfigs(hosts, clusterName)

    // 启动三个节点
    val system1 = ActorSystem(RaftNode(Follower, clusterName, hosts, 0), clusterName, configs(0))
    val system2 = ActorSystem(RaftNode(Follower, clusterName, hosts, 1), clusterName, configs(1))
    val system3 = ActorSystem(RaftNode(Follower, clusterName, hosts, 2), clusterName, configs(2))

    Thread.sleep(5000)
//    system1 ! Heartbeat
//    system2 ! Heartbeat
//    system3 ! Heartbeat
  }
}
