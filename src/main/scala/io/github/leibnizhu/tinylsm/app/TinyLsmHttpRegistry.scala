package io.github.leibnizhu.tinylsm.app

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import io.github.leibnizhu.tinylsm.TinyLsm
import io.github.leibnizhu.tinylsm.app.TinyLsmHttpRegistry.Command
import io.github.leibnizhu.tinylsm.mvcc.Transaction

import java.util.concurrent.ConcurrentHashMap


class TinyLsmHttpRegistry(state: TinyLsmHttpRegistry.InnerState) {

  def registry(): Behavior[Command] =
    Behaviors.receiveMessage {
      (_: Command).behavior(state)
    }
}

object TinyLsmHttpRegistry {
  case class InnerState(storage: TinyLsm, transactions: java.util.Map[Int, Transaction])

  def apply(storage: TinyLsm, transactions: ConcurrentHashMap[Int, Transaction]): TinyLsmHttpRegistry =
    new TinyLsmHttpRegistry(InnerState(storage, transactions))

  sealed trait Command {
    def behavior(state: InnerState): Behavior[Command]
  }

  final case class CommonResponse(content: String, status: Int)

  final case class GetByKey(key: String, tid: Option[Int], replyTo: ActorRef[CommonResponse]) extends Command:
    override def behavior(state: InnerState): Behavior[Command] = {
      val response = tid match
        case None =>
          state.storage.get(key).map(CommonResponse(_, 200))
            .getOrElse(CommonResponse("KeyNotExists", 404))
        case Some(tid) =>
          val transaction = state.transactions.get(tid)
          if (transaction == null) {
            CommonResponse(s"Transaction with ID=$tid is not exists", 404)
          } else if (transaction.isCommited) {
            CommonResponse(s"Transaction with ID=$tid is already committed or rollback", 400)
          } else {
            transaction.get(key).map(CommonResponse(_, 200))
              .getOrElse(CommonResponse("KeyNotExists", 404))
          }
      replyTo ! response
      Behaviors.same
    }


  final case class PutValue(key: String, value: String, tid: Option[Int], replyTo: ActorRef[CommonResponse]) extends Command:
    override def behavior(state: InnerState): Behavior[Command] = {
      val response = tid match
        case None =>
          state.storage.put(key, value)
          CommonResponse("", 204)
        case Some(tid) =>
          val transaction = state.transactions.get(tid)
          if (transaction == null) {
            CommonResponse(s"Transaction with ID=$tid is not exists", 404)
          } else if (transaction.isCommited) {
            CommonResponse(s"Transaction with ID=$tid is already committed or rollback", 400)
          } else {
            transaction.put(key, value)
            CommonResponse("", 204)
          }
      replyTo ! response
      Behaviors.same
    }

  final case class DeleteByKey(key: String, tid: Option[Int], replyTo: ActorRef[CommonResponse]) extends Command:
    override def behavior(state: InnerState): Behavior[Command] = {
      val response = tid match
        case None =>
          state.storage.delete(key)
          CommonResponse("", 204)
        case Some(tid) =>
          val transaction = state.transactions.get(tid)
          if (transaction == null) {
            CommonResponse(s"Transaction with ID=$tid is not exists", 404)
          } else if (transaction.isCommited) {
            CommonResponse(s"Transaction with ID=$tid is already committed or rollback", 400)
          } else {
            transaction.delete(key)
            CommonResponse("", 204)
          }
      Behaviors.same
    }
}