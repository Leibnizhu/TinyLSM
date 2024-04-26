package io.github.leibnizhu.tinylsm.app

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import akka.http.scaladsl.server.Directives.complete
import akka.http.scaladsl.server.StandardRoute
import io.github.leibnizhu.tinylsm.TinyLsm
import io.github.leibnizhu.tinylsm.app.BizCode.*
import io.github.leibnizhu.tinylsm.app.TinyLsmHttpRegistry.Command
import io.github.leibnizhu.tinylsm.mvcc.Transaction
import io.github.leibnizhu.tinylsm.utils.Bound

import java.util.StringJoiner


class TinyLsmHttpRegistry(state: TinyLsmHttpRegistry.InnerState) {

  def registry(): Behavior[Command] =
    Behaviors.receiveMessage {
      (_: Command).executeAndReply(state)
    }
}

object TinyLsmHttpRegistry {
  case class InnerState(storage: TinyLsm, transactions: java.util.Map[Int, Transaction])

  def apply(storage: TinyLsm, transactions: java.util.Map[Int, Transaction]): TinyLsmHttpRegistry =
    new TinyLsmHttpRegistry(InnerState(storage, transactions))

  sealed trait Command {
    val replyTo: ActorRef[CommonResponse]

    final def executeAndReply(state: InnerState): Behavior[Command] = {
      replyTo ! wrapBehavior(state)
      Behaviors.same
    }

    final def wrapBehavior(state: InnerState): CommonResponse = {
      try {
        doBehavior(state)
      } catch
        case e: RuntimeException =>
          CommonResponse(s"Request Error: ${e.getMessage}", 500)
        case e: Exception =>
          CommonResponse("Server Error", 500)
    }

    def doBehavior(state: InnerState): CommonResponse
  }

  final case class CommonResponse(content: String, httpCode: Int, bizCode: BizCode = Success) {
    def response(): StandardRoute = complete(httpCode, content)
  }

  final case class GetByKey(key: String, tid: Option[Int], replyTo: ActorRef[CommonResponse]) extends Command:
    override def doBehavior(state: InnerState): CommonResponse = tid match
      case None =>
        state.storage.get(key).map(CommonResponse(_, 200))
          .getOrElse(CommonResponse("KeyNotExists", 404, KeyNotExists))
      case Some(tid) =>
        val transaction = state.transactions.get(tid)
        if (transaction == null) {
          CommonResponse(s"Transaction with ID=$tid is not exists", 404, TransactionNotExists)
        } else if (transaction.isCommited) {
          CommonResponse(s"Transaction with ID=$tid is already committed or rollback", 400, TransactionInvalid)
        } else {
          transaction.get(key).map(CommonResponse(_, 200))
            .getOrElse(CommonResponse("KeyNotExists", 404, KeyNotExists))
        }


  final case class PutValue(key: String, value: String, tid: Option[Int], replyTo: ActorRef[CommonResponse]) extends Command:
    override def doBehavior(state: InnerState): CommonResponse = tid match
      case None =>
        state.storage.put(key, value)
        CommonResponse("", 204)
      case Some(tid) =>
        val transaction = state.transactions.get(tid)
        if (transaction == null) {
          CommonResponse(s"Transaction with ID=$tid is not exists", 404, TransactionNotExists)
        } else if (transaction.isCommited) {
          CommonResponse(s"Transaction with ID=$tid is already committed or rollback", 400, TransactionInvalid)
        } else {
          transaction.put(key, value)
          CommonResponse("", 204)
        }

  final case class DeleteByKey(key: String, tid: Option[Int], replyTo: ActorRef[CommonResponse]) extends Command:
    override def doBehavior(state: InnerState): CommonResponse = tid match
      case None =>
        state.storage.delete(key)
        CommonResponse("", 204)
      case Some(tid) =>
        val transaction = state.transactions.get(tid)
        if (transaction == null) {
          CommonResponse(s"Transaction with ID=$tid is not exists", 404, TransactionNotExists)
        } else if (transaction.isCommited) {
          CommonResponse(s"Transaction with ID=$tid is already committed or rollback", 400, TransactionInvalid)
        } else {
          transaction.delete(key)
          CommonResponse("", 204)
        }

  final case class ScanRange(fromType: String, fromKey: String, toType: String, toKey: String,
                             tid: Option[Int], replyTo: ActorRef[CommonResponse]) extends Command:
    override def doBehavior(state: InnerState): CommonResponse = {
      val lower = Bound(fromType, fromKey)
      val upper = Bound(toType, toKey)
      tid match
        case None =>
          val itr = state.storage.scan(lower, upper)
          val sj = new StringJoiner("\n")
          CommonResponse(itr.joinAllKeyValue(sj).toString, 200)
        case Some(tid) =>
          val transaction = state.transactions.get(tid)
          if (transaction == null) {
            CommonResponse(s"Transaction with ID=$tid is not exists", 404, TransactionNotExists)
          } else if (transaction.isCommited) {
            CommonResponse(s"Transaction with ID=$tid is already committed or rollback", 400, TransactionInvalid)
          } else {
            val itr = transaction.scan(lower, upper)
            val sj = new StringJoiner("\n")
            CommonResponse(itr.joinAllKeyValue(sj).toString, 200)
          }
    }

  final case class State(replyTo: ActorRef[CommonResponse]) extends Command:
    override def doBehavior(state: InnerState): CommonResponse = {
      val status = state.storage.inner.dumpState()
      CommonResponse(status, 200)
    }

  final case class Flush(replyTo: ActorRef[CommonResponse]) extends Command:
    override def doBehavior(state: InnerState): CommonResponse = {
      state.storage.forceFlush()
      CommonResponse("Flush success", 204)
    }

  final case class CreateTxn(replyTo: ActorRef[CommonResponse]) extends Command:
    override def doBehavior(state: InnerState): CommonResponse = {
      val txn = state.storage.newTxn()
      state.transactions.put(txn.tid, txn)
      CommonResponse(txn.tid.toString, 200)
    }

  final case class CommitTxn(tid: Int, replyTo: ActorRef[CommonResponse]) extends Command:
    override def doBehavior(state: InnerState): CommonResponse = {
      val transaction = state.transactions.remove(tid)
      if (transaction == null) {
        CommonResponse(s"Transaction with ID=$tid is not exists", 404, TransactionNotExists)
      } else if (transaction.isCommited) {
        CommonResponse(s"Transaction with ID=$tid is already committed or rollback", 400, TransactionInvalid)
      } else
        try {
          transaction.commit()
          CommonResponse(s"Commit Transaction success", 204)
        } catch
          case e: IllegalStateException => CommonResponse(e.getMessage, 409, TransactionCommitFailed)
          case e: Exception => CommonResponse("TinyLSM Server error", 500, CommonError)
    }

  final case class RollbackTxn(tid: Int, replyTo: ActorRef[CommonResponse]) extends Command:
    override def doBehavior(state: InnerState): CommonResponse = {
      val transaction = state.transactions.remove(tid)
      if (transaction == null) {
        CommonResponse(s"Transaction with ID=$tid is not exists", 404, TransactionNotExists)
      } else if (transaction.isCommited) {
        CommonResponse(s"Transaction with ID=$tid is already committed or rollback", 400, TransactionInvalid)
      } else
        try {
          transaction.rollback()
          CommonResponse(s"Rollback Transaction(ID=$tid) success", 204)
        } catch
          case e: Exception => CommonResponse("TinyLSM Server error", 500, CommonError)
    }
}

enum BizCode(val code: Int) {
  case Success extends BizCode(10000)
  case KeyNotExists extends BizCode(20001)
  case TransactionNotExists extends BizCode(20002)
  case TransactionInvalid extends BizCode(20003)
  case TransactionCommitFailed extends BizCode(20004)
  case CommonError extends BizCode(99999)
}