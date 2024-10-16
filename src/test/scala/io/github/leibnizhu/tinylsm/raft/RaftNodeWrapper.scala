package io.github.leibnizhu.tinylsm.raft

import com.typesafe.config.Config
import org.apache.pekko.actor.typed.scaladsl.AskPattern
import org.apache.pekko.actor.typed.scaladsl.AskPattern.*
import org.slf4j.LoggerFactory

import _root_.scala.runtime.stdLibPatches.Predef.assert
import scala.reflect.ClassTag

case class RaftNodeWrapper(clusterName: String, hostsStr: String, curIdx: Int) extends RaftApp {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def getState: RaftState = askThisNode[QueryStateResponse](ref => QueryStateRequest(ref)).state

  override def applyCommand(index: Int, command: Array[Byte]): Unit = {
    logger.info("Node{} is applying command(index={}): {}", curIdx, index, new String(command))
  }

  override def applySnapshot(index: Int, term: Int, snapshot: Array[Byte]): Unit = {
    logger.info("Node{} is applying snapshot, last log {}@{}, snapshot length={}", curIdx, index, term, snapshot.length)
  }
}

