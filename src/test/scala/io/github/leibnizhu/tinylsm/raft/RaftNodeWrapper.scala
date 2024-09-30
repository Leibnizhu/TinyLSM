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
  var system: ActorSystem[Command] = _

  implicit val timeout: Timeout = 3.seconds

  def start(): Unit = {
    system = ActorSystem(RaftNode(Follower, clusterName, hosts, curInx), clusterName, configs(curInx))
  }

  def getState: RaftState = {
    implicit val scheduler: Scheduler = system.scheduler
    Await.result(system.ask(ref => QueryStateRequest(ref)), 3.seconds).asInstanceOf[QueryStateResponse].state
  }

  def stop(): Unit = {
    system.terminate()
  }
}

