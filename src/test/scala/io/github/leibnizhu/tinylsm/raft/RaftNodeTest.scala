package io.github.leibnizhu.tinylsm.raft

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.pekko.actor.typed.scaladsl.AskPattern
import org.apache.pekko.actor.typed.scaladsl.AskPattern.*
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.LoggerFactory

import _root_.scala.runtime.stdLibPatches.Predef.assert

class RaftNodeTest extends AnyFunSuite {
  private val logger = LoggerFactory.getLogger(this.getClass)

  test("normal_3_nodes_election") {
    val clusterName = "TinyLsmCluster"
    val hosts = "localhost:2550,localhost:2551,localhost:2552".split(",")
    val cluster = RaftCluster(clusterName, hosts)

    // 启动所有节点，等待选举结束
    cluster.startAll()
    Thread.sleep(10000)
    val curTerm = cluster.currentLeader()._2
    assert(curTerm < 20, "应该在20轮任期内完成Leader选举")
    cluster.stopAll()
  }

  test("normal_5_nodes_election") {
    val clusterName = "TinyLsmCluster"
    val hosts = "localhost:2550,localhost:2551,localhost:2552,localhost:2553,localhost:2554".split(",")
    val cluster = RaftCluster(clusterName, hosts)

    // 启动所有节点，等待选举结束
    cluster.startAll()
    Thread.sleep(10000)
    val curTerm = cluster.currentLeader()._2
    assert(curTerm < 20, "应该在20轮任期内完成Leader选举")
    cluster.stopAll()
  }

  test("leader_stop_3_nodes_election") {
    val clusterName = "TinyLsmCluster"
    val hosts = "localhost:2550,localhost:2551,localhost:2552".split(",")
    val cluster = RaftCluster(clusterName, hosts)

    // 启动所有节点，等待选举结束
    cluster.startAll()
    Thread.sleep(5000)

    // 停止两次leader
    val (leader1, leaderState1) = cluster.stopCurrentLeader()
    Thread.sleep(3000)
    val (leader2, leaderState2) = cluster.stopCurrentLeader()
    Thread.sleep(3000)

    // 启动停止的节点
    logger.info("==> Node {} is starting", leaderState1.name())
    leader1.start()
    Thread.sleep(5000)
    logger.info("==> Node {} is starting", leaderState2.name())
    leader2.start()
    Thread.sleep(5000)

    val curTerm = cluster.currentLeader()._2
    assert(curTerm < 50, "应该在50轮任期内完成3次Leader选举")
    cluster.stopAll()
  }

  test("normal_3_nodes_append_log") {
    val clusterName = "TinyLsmCluster"
    val hosts = "localhost:2550,localhost:2551,localhost:2552".split(",")
    val cluster = RaftCluster(clusterName, hosts)

    // 启动所有节点，等待选举结束
    cluster.startAll()
    Thread.sleep(5000)

    val oldLeader = cluster.currentLeader()._1
    oldLeader.system ! ClientRequest("ping".getBytes)
    Thread.sleep(3000)

    val oldLeaderState = oldLeader.getState
    logger.info("==> Current Leader is {}, matchIndex: {}, nextIndex: {}, stopping",
      oldLeaderState.name(), oldLeaderState.nextIndex.mkString(","), oldLeaderState.matchIndex.mkString(","))
    oldLeader.stop()
    Thread.sleep(3000)
    oldLeader.start()
    Thread.sleep(5000)

    val newLeader = cluster.currentLeader()._1
    val newLeaderState = newLeader.getState
    assert(oldLeaderState.matchIndex.sameElements(newLeaderState.matchIndex))
    assert(oldLeaderState.nextIndex.sameElements(newLeaderState.nextIndex))
    cluster.stopAll()
  }


  test("leader_stop_and_recover_3_nodes_election") {
    val clusterName = "TinyLsmCluster"
    val hosts = "localhost:2550,localhost:2551,localhost:2552".split(",")
    System.setProperty("raft.persistor", PersistorFactory.MEMORY)
    val cluster = RaftCluster(clusterName, hosts)

    // 启动所有节点，等待选举结束
    cluster.startAll()
    Thread.sleep(5000)

    // 停止两次leader
    val (leader1, leaderState1) = cluster.stopCurrentLeader()
    Thread.sleep(3000)
    val (leader2, leaderState2) = cluster.stopCurrentLeader()
    Thread.sleep(3000)

    // 启动停止的节点
    logger.info("==> Node {} is starting", leaderState1.name())
    leader1.start(Some(leaderState1.persistor))
    Thread.sleep(5000)
    logger.info("==> Node {} is starting", leaderState2.name())
    leader2.start(Some(leaderState2.persistor))
    Thread.sleep(5000)

    val curTerm = cluster.currentLeader()._2
    assert(curTerm < 50, "应该在50轮任期内完成3次Leader选举")

    // persist不为空
    assert(leaderState1.persistor.readPersist().nonEmpty)
    assert(leaderState2.persistor.readPersist().nonEmpty)
    cluster.stopAll()
    System.clearProperty("raft.persistor")
  }
}
