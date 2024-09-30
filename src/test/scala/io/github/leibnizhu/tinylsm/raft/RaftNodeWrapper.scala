package io.github.leibnizhu.tinylsm.raft

import akka.actor.typed.scaladsl.AskPattern
import akka.actor.typed.scaladsl.AskPattern.*
import akka.actor.typed.{ActorSystem, Scheduler}
import akka.util.Timeout
import com.typesafe.config.Config

import _root_.scala.concurrent.Await
import _root_.scala.runtime.stdLibPatches.Predef.assert
import scala.concurrent.duration.*

case class RaftNodeWrapper(clusterName: String, configs: Array[Config], curInx: Int) {
  val hosts: Array[String] = configs.map(c => c.getString("akka.remote.artery.canonical.hostname") + ":" + c.getString("akka.remote.artery.canonical.port"))
  val system: ActorSystem[Command] = ActorSystem(RaftNode(Follower, clusterName, hosts, curInx), clusterName, configs(curInx))

  implicit val timeout: Timeout = 3.seconds
  implicit val scheduler: Scheduler = system.scheduler

  def getState: RaftState = {
    Await.result(system.ask(ref => QueryStateRequest(ref)), 3.seconds).asInstanceOf[QueryStateResponse].state
  }
}

