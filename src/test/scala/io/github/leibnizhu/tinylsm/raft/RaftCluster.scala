package io.github.leibnizhu.tinylsm.raft

import com.typesafe.config.Config
import io.github.leibnizhu.tinylsm.raft.RaftRole.Leader
import org.slf4j.LoggerFactory

import scala.concurrent.duration.*
import scala.util.control.Breaks.*

case class RaftCluster(clusterName: String, hostNum: Int) {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val startPort = 25550

  val hosts: Array[String] = (0 until hostNum).map(i => s"localhost:${startPort + i}").toArray
  val configs: Array[Config] = RaftApp.clusterConfigs(hosts)
  var nodes: Array[RaftNodeWrapper] = _
  var persistors: Array[Persistor] = (0 until hostNum).map(i => MemoryPersistor(i)).toArray

  def start(): Unit = {
    nodes = configs.indices.toArray.map(i => {
      val wrapper = RaftNodeWrapper(clusterName, hosts.mkString(","), i)
      wrapper.start(Some(persistors(i)))
      Thread.sleep(100)
      wrapper
    })
    updateAllActorSystem()
  }

  private def updateAllActorSystem(): Unit = {
    val allSystem = nodes.map(n => if (n.stopped) null else n.system)
    nodes.foreach(n => n.allSystem = allSystem)
  }

  def start(nodeIdx: Int): Unit = {
    logger.info("===> Starting Node{}", nodeIdx)
    nodes(nodeIdx).start(Some(persistors(nodeIdx)))
    // 等待启动
    Thread.sleep(2000)
    nodes((nodeIdx + 1) % nodes.length).tellOtherNode(nodeIdx, RenewState())
    logger.info("==> Node{}'s state: {}", nodeIdx, nodes(nodeIdx).getState)
    updateAllActorSystem()
  }

  def stop(nodeIdx: Int): Unit = {
    logger.info("<== Stopping Node{}", nodeIdx)
    nodes(nodeIdx).stop()
    updateAllActorSystem()
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
    assert(leaderCount > 0, "未选举出Leader")
    assert(leaderCount <= 1, "有且只能有一个Leader")
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

    while (System.nanoTime() - t0 < timeout.toNanos) {
      // 尝试所有的服务器，可能有一个是领导者。
      var index = -1
      for (si <- hosts.indices) breakable {
        starts = (starts + 1) % hosts.length
        val rf = nodes(starts)
        if (rf != null && !rf.stopped) {
          val response = rf.askSelf[CommandResponse](ref => CommandRequest(cmd, ref))
          if (response.isLeader) {
            index = response.index
            break
          }
        }
      }

      if (index != -1) {
        logger.info("sendOneCommand() detected Node{} declaim it's Leader, new log index={}", starts, index)
        // 有人声称自己是领导者，并提交了我们的命令；等待一段时间以达成一致。
        val t1 = System.nanoTime()
        val agreementTimeout = 5.seconds

        var agreed = false
        while (System.nanoTime() - t1 < agreementTimeout.toNanos) {
          val (nd, cmd1) = nCommitted(index)
          //          logger.info("sendOneCommand() get committed command {} in {} servers", new String(cmd), nd)
          if (nd > 0 && nd >= expectedServers) {
            // 已达成一致
            if (cmd1 sameElements cmd) {
              // 而且是我们提交的命令。
              logger.info("sendOneCommand() success, index={}, command={}", index, new String(cmd))
              return index
            } else {
              logger.info("sendOneCommand() detected command not same: committed={}, origin={}", new String(cmd1), new String(cmd))
            }
          }
          Thread.sleep(100)
        }
        println(s"$index, nodes' log range=${nodes.map(n => (n.curIdx, if (n.logMap.isEmpty) "" else s"${n.logMap.keys.min} -> ${n.logMap.keys.max}")).mkString("(", ", ", ")")}")

        if (!retry) {
          throw new AssertionError(s"sendOneCommand(${new String(cmd)}) failed to reach agreement")
        }
      } else {
        logger.info("sendOneCommand() can't detect a Leader")
        Thread.sleep(200)
      }
    }
    throw new AssertionError(s"sendOneCommand(${new String(cmd)})  failed to reach agreement")
  }

  def nCommitted(index: Int): (Int, Array[Byte]) = {
    var count = 0
    var cmd: Array[Byte] = null

    for (i <- nodes.indices) {
      // 确保索引有效
      val cmd1 = nodes(i).logMap.get(index).orNull
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

  def logSize(): Int = {
    nodes.filter(!_.stopped).map(_.getState.persistor.readPersist())
      .filter(_.isDefined).map(_.get.logSize()).max
  }
}
