package io.github.leibnizhu.tinylsm.raft

import org.apache.pekko.actor.typed.scaladsl.AskPattern.*
import org.apache.pekko.actor.typed.scaladsl.{AskPattern, Behaviors}
import org.apache.pekko.actor.typed.{ActorSystem, Scheduler}
import org.apache.pekko.util.Timeout
import com.typesafe.config.Config
import org.slf4j.LoggerFactory

import _root_.scala.concurrent.Await
import _root_.scala.runtime.stdLibPatches.Predef.assert
import scala.concurrent.duration.*

case class RaftNodeWrapper(clusterName: String, configs: Array[Config], curIdx: Int) {
  private val logger = LoggerFactory.getLogger(this.getClass)
  val hosts: Array[String] = configs.map(c => c.getString("pekko.remote.artery.canonical.hostname") + ":" + c.getString("pekko.remote.artery.canonical.port"))
  var system: ActorSystem[Command] = _

  implicit val timeout: Timeout = 3.seconds

  def start(persistorOption: Option[Persistor] = None): Unit = {
    val persistor = persistorOption.getOrElse(PersistorFactory.byConfig(curIdx))
    system = ActorSystem(RaftNode(Follower, clusterName, hosts, curIdx, persistor), clusterName, configs(curIdx))
    system.systemActorOf(Behaviors.receive { (context, message) => {
      message match {
        case ap: ApplyLogRequest =>
          logger.info("Applying Log: {}", ap)
          Behaviors.same
      }
    }
    }, "applyLog")
  }

  def getState: RaftState = {
    implicit val scheduler: Scheduler = system.scheduler
    Await.result(system.ask(ref => QueryStateRequest(ref)), 3.seconds).asInstanceOf[QueryStateResponse].state
  }

  def stop(): Unit = {
    system.terminate()
    Await.result(system.whenTerminated, Duration.Inf)
    system = null
  }

  def stopped: Boolean = system == null
}

