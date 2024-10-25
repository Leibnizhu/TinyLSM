package io.github.leibnizhu.tinylsm.raft

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.LoggerFactory

import java.util.concurrent.ThreadLocalRandom
import _root_.scala.runtime.stdLibPatches.Predef.assert

class RaftNodeTest extends AnyFunSuite with BeforeAndAfterEach with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    // 在 Github workflow 运行时经常绑定不了端口
    assume(!sys.env.get("GITHUB_ACTIONS").contains("true"), "Test ignored in GitHub Actions environment")
  }

  private val logger = LoggerFactory.getLogger(this.getClass)
  private val clusterName = "TinyLsmClusterTest"

  private var cluster: RaftCluster = _

  override def afterEach(): Unit = {
    if (cluster != null) {
      cluster.stop()
    }
  }

  test("normal_3_nodes_election") {
    cluster = RaftCluster(clusterName, 3)
    // 启动所有节点，等待选举结束
    cluster.start()
    Thread.sleep(10000)

    val curTerm = cluster.currentLeader()._2
    assert(curTerm < 20, "应该在20轮任期内完成Leader选举")
  }

  test("normal_5_nodes_election") {
    cluster = RaftCluster(clusterName, 5)

    // 启动所有节点，等待选举结束
    cluster.start()
    Thread.sleep(10000)
    val curTerm = cluster.currentLeader()._2
    assert(curTerm < 20, "应该在20轮任期内完成Leader选举")
  }

  test("leader_stop_3_nodes_election") {
    cluster = RaftCluster(clusterName, 3)
    // 启动所有节点，等待选举结束
    cluster.start()
    Thread.sleep(5000)

    // 停止两次leader
    val (leader1, leaderState1) = cluster.stopCurrentLeader()
    Thread.sleep(3000)
    val (leader2, leaderState2) = cluster.stopCurrentLeader()
    Thread.sleep(5000)

    // 启动停止的节点
    logger.info("==> Node {} is starting", leaderState1.name())
    leader1.start()
    Thread.sleep(5000)
    logger.info("==> Node {} is starting", leaderState2.name())
    leader2.start()
    Thread.sleep(5000)

    val curTerm = cluster.currentLeader()._2
    assert(curTerm < 50, "应该在50轮任期内完成3次Leader选举")
  }

  test("normal_3_nodes_append_log") {
    cluster = RaftCluster(clusterName, 3)
    // 启动所有节点，等待选举结束
    cluster.start()
    Thread.sleep(5000)

    val oldLeader = cluster.currentLeader()._1
    oldLeader.addCommand("ping".getBytes)
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
    assertResult(oldLeaderState.matchIndex)(newLeaderState.matchIndex)
    assertResult(oldLeaderState.nextIndex)(newLeaderState.nextIndex)
  }

  test("3_nodes_basic_agree") {
    cluster = RaftCluster(clusterName, 3)
    // 启动所有节点，等待选举结束
    cluster.start()
    Thread.sleep(5000)
    for (index <- 0 until 10) {
      val (nd, _) = cluster.nCommitted(index)
      assert(nd <= 0, "some have committed before Start()")
      val xindex = cluster.sendOneCommand(s"${index * 100}".getBytes, 3, false)
      assert(xindex == index, s"got index $xindex but expected $index")
    }
  }


  test("leader_stop_and_recover_3_nodes_election") {
    System.setProperty("raft.persistor", PersistorFactory.MEMORY)

    cluster = RaftCluster(clusterName, 3)
    // 启动所有节点，等待选举结束
    cluster.start()
    Thread.sleep(5000)

    // 停止两次leader
    val (leader1, leaderState1) = cluster.stopCurrentLeader()
    Thread.sleep(3000)
    val (leader2, leaderState2) = cluster.stopCurrentLeader()
    Thread.sleep(5000)

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
    assert(leaderState1.persistor.doReadPersist().nonEmpty)
    assert(leaderState2.persistor.doReadPersist().nonEmpty)

    System.clearProperty("raft.persistor")
  }


  private def snapshot_commons_test(stop: Boolean): Unit = {
    val servers = 3
    cluster = RaftCluster(clusterName, servers)
    cluster.start()
    Thread.sleep(5000)
    cluster.sendOneCommand(randomCommand(), servers, true)
    var leaderIdx = cluster.currentLeader()._1.curIdx

    for (i <- 0 until 15) {
      val (victim, sender) =
        if (i % 3 == 1) (leaderIdx, (leaderIdx + 1) % servers)
        else ((leaderIdx + 1) % servers, leaderIdx)
      if (stop) {
        logger.info("==> {} iteration stop Node{}", i, victim)
        cluster.stop(victim)
        cluster.sendOneCommand(randomCommand(), servers - 1, true)
      }
      for (j <- 0 to RaftNodeWrapper.snapShotInterval) {
        cluster.nodes(sender).addCommand(randomCommand())
      }
      cluster.sendOneCommand(randomCommand(), servers - 1, true)
      val logSize = cluster.logSize()
      assert(logSize <= 20000, "Log size too large")
      if (stop) {
        cluster.start(victim)
        Thread.sleep(2000)
        cluster.sendOneCommand(randomCommand(), servers, true)
        leaderIdx = cluster.currentLeader()._1.curIdx
      }
    }
  }

  test("3_nodes_basic_snapshot") {
    snapshot_commons_test(false)
  }

  test("3_nodes_stop_snapshot") {
    snapshot_commons_test(true)
  }

  // 启动所有节点，等待选举结束
  private def randomCommand() = ThreadLocalRandom.current().nextInt().toString.getBytes
}
