package io.github.leibnizhu.tinylsm.app

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import io.github.leibnizhu.tinylsm.TinyLsm
import io.github.leibnizhu.tinylsm.app.ApiCommands.{Command, InnerState}
import io.github.leibnizhu.tinylsm.app.BizCode.*
import io.github.leibnizhu.tinylsm.mvcc.Transaction
import org.slf4j.LoggerFactory


class TinyLsmHttpRegistry(state: ApiCommands.InnerState) {
  private val log = LoggerFactory.getLogger(this.getClass)

  def registry(): Behavior[Command] =
    Behaviors.receiveMessage {
      case c: Command => c.executeAndReply(state)
      case msg =>
        log.error("Unsupported {} msg: {}", msg.getClass.getName, msg)
        Behaviors.ignore
    }
}

object TinyLsmHttpRegistry {
  def apply(storage: TinyLsm, transactions: java.util.Map[Int, Transaction]): TinyLsmHttpRegistry =
    new TinyLsmHttpRegistry(InnerState(storage, transactions))
}
