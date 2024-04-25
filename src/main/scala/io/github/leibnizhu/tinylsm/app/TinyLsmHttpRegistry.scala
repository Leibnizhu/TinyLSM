package io.github.leibnizhu.tinylsm.app

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import akka.http.scaladsl.server.Directives.complete
import akka.http.scaladsl.server.StandardRoute
import io.github.leibnizhu.tinylsm.TinyLsm
import io.github.leibnizhu.tinylsm.app.TinyLsmHttpRegistry.Command
import io.github.leibnizhu.tinylsm.mvcc.Transaction
import io.github.leibnizhu.tinylsm.utils.Bound

import java.util.StringJoiner
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
    val replyTo: ActorRef[CommonResponse]

    final def behavior(state: InnerState): Behavior[Command] = try {
      doBehavior(state)
    } catch
      case e: RuntimeException =>
        replyTo ! CommonResponse(s"Request Error: ${e.getMessage}", 500)
        Behaviors.same
      case e: Exception =>
        replyTo ! CommonResponse("Server Error", 500)
        Behaviors.same

    def doBehavior(state: InnerState): Behavior[Command]
  }

  final case class CommonResponse(content: String, status: Int) {
    def response(): StandardRoute = complete(status, content)
  }

  final case class GetByKey(key: String, tid: Option[Int], replyTo: ActorRef[CommonResponse]) extends Command:
    override def doBehavior(state: InnerState): Behavior[Command] = {
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
    override def doBehavior(state: InnerState): Behavior[Command] = {
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
    override def doBehavior(state: InnerState): Behavior[Command] = {
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
      replyTo ! response
      Behaviors.same
    }

  final case class ScanRange(fromType: String, fromKey: String, toType: String, toKey: String,
                             tid: Option[Int], replyTo: ActorRef[CommonResponse]) extends Command:
    override def doBehavior(state: InnerState): Behavior[Command] = {
      val lower = Bound(fromType, fromKey)
      val upper = Bound(toType, toKey)
      val response = tid match
        case None =>
          val itr = state.storage.scan(lower, upper)
          val sj = new StringJoiner("\n")
          CommonResponse(itr.joinAllKeyValue(sj).toString, 200)
        case Some(tid) =>
          val transaction = state.transactions.get(tid)
          if (transaction == null) {
            CommonResponse(s"Transaction with ID=$tid is not exists", 404)
          } else if (transaction.isCommited) {
            CommonResponse(s"Transaction with ID=$tid is already committed or rollback", 400)
          } else {
            val itr = transaction.scan(lower, upper)
            val sj = new StringJoiner("\n")
            CommonResponse(itr.joinAllKeyValue(sj).toString, 200)
          }
      replyTo ! response
      Behaviors.same
    }

  final case class State(replyTo: ActorRef[CommonResponse]) extends Command:
    override def doBehavior(state: InnerState): Behavior[Command] = {
      val status = state.storage.inner.dumpState()
      replyTo ! CommonResponse(status, 200)
      Behaviors.same
    }

  final case class Flush(replyTo: ActorRef[CommonResponse]) extends Command:
    override def doBehavior(state: InnerState): Behavior[Command] = {
      state.storage.forceFlush()
      replyTo ! CommonResponse("Flush success", 204)
      Behaviors.same
    }

  final case class CreateTxn(replyTo: ActorRef[CommonResponse]) extends Command:
    override def doBehavior(state: InnerState): Behavior[Command] = {
      val txn = state.storage.newTxn()
      state.transactions.put(txn.tid, txn)
      replyTo ! CommonResponse(txn.tid.toString, 200)
      Behaviors.same
    }

  final case class CommitTxn(tid: Int, replyTo: ActorRef[CommonResponse]) extends Command:
    override def doBehavior(state: InnerState): Behavior[Command] = {
      val transaction = state.transactions.remove(tid)
      val response = if (transaction == null) {
        CommonResponse(s"Transaction with ID=$tid is not exists", 404)
      } else if (transaction.isCommited) {
        CommonResponse(s"Transaction with ID=$tid is already committed or rollback", 400)
      } else
        try {
          transaction.commit()
          CommonResponse(s"Commit Transaction success", 204)
        } catch
          case e: IllegalStateException => CommonResponse(e.getMessage, 409)
          case e: Exception => CommonResponse("TinyLSM Server error", 500)
      replyTo ! response
      Behaviors.same
    }

  final case class RollbackTxn(tid: Int, replyTo: ActorRef[CommonResponse]) extends Command:
    override def doBehavior(state: InnerState): Behavior[Command] = {
      val transaction = state.transactions.remove(tid)
      val response = if (transaction == null) {
        CommonResponse(s"Transaction with ID=$tid is not exists", 404)
      } else if (transaction.isCommited) {
        CommonResponse(s"Transaction with ID=$tid is already committed or rollback", 400)
      } else
        try {
          transaction.rollback()
          CommonResponse(s"Rollback Transaction(ID=$tid) success", 204)
        } catch
          case e: Exception => CommonResponse("TinyLSM Server error", 500)
      replyTo ! response
      Behaviors.same
    }
}