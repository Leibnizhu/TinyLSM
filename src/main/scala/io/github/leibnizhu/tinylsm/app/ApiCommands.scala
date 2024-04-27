package io.github.leibnizhu.tinylsm.app

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import akka.http.scaladsl.server.Directives.complete
import akka.http.scaladsl.server.StandardRoute
import io.github.leibnizhu.tinylsm.app.BizCode.*
import io.github.leibnizhu.tinylsm.iterator.StorageIterator
import io.github.leibnizhu.tinylsm.mvcc.Transaction
import io.github.leibnizhu.tinylsm.utils.Bound
import io.github.leibnizhu.tinylsm.{Key, MemTableEntry, MemTableValue, RawKey, TinyLsm}

import java.util.StringJoiner

object ApiCommands {
  case class InnerState(storage: TinyLsm, transactions: java.util.Map[Int, Transaction])

  final case class CommonResponse(data: Any, message: String, httpCode: Int, bizCode: BizCode = Success) {
    def response(): StandardRoute = data match
      case null => complete(httpCode, "")
      case s: String => complete(httpCode, s)
      case bs: MemTableValue => complete(httpCode, new String(bs))
      case list: List[_] =>
        val sj = new StringJoiner("\n")
        for (element <- list) {
          element match
            case (k: Key, v: MemTableValue) =>
              sj.add(new String(k.bytes)).add(new String(v))
            case _ => sj.add(element.toString)
        }
        complete(httpCode, sj.toString)
      case o: Any => complete(httpCode, o.toString)
  }

  object CommonResponse {
    def error(message: String, httpCode: Int, bizCode: BizCode = CommonError): CommonResponse = {
      new CommonResponse(null.asInstanceOf, message, httpCode, bizCode)
    }

    def success(data: Any): CommonResponse = {
      new CommonResponse(data, null, 200, Success)
    }

    def emptySuccess(message: String = null): CommonResponse = {
      new CommonResponse(null, message, 204, Success)
    }
  }

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
          CommonResponse.error(s"Request Error: ${e.getMessage}", 500)
        case e: Exception =>
          CommonResponse.error("Server Error", 500)
    }

    def doBehavior(state: InnerState): CommonResponse
  }

  final case class GetByKey(key: Array[Byte], tid: Option[Int],
                            replyTo: ActorRef[CommonResponse]) extends Command:
    override def doBehavior(state: InnerState): CommonResponse = tid match
      case None =>
        state.storage.get(key).map(CommonResponse.success)
          .getOrElse(CommonResponse.error("KeyNotExists", 404, KeyNotExists))
      case Some(tid) =>
        val transaction = state.transactions.get(tid)
        if (transaction == null) {
          CommonResponse.error(s"Transaction with ID=$tid is not exists", 404, TransactionNotExists)
        } else if (transaction.isCommited) {
          CommonResponse.error(s"Transaction with ID=$tid is already committed or rollback", 400, TransactionInvalid)
        } else {
          transaction.get(key).map(CommonResponse.success)
            .getOrElse(CommonResponse.error("KeyNotExists", 404, KeyNotExists))
        }


  final case class PutValue(key: Array[Byte], value: Array[Byte], tid: Option[Int],
                            replyTo: ActorRef[CommonResponse]) extends Command:
    override def doBehavior(state: InnerState): CommonResponse = tid match
      case None =>
        state.storage.put(key, value)
        CommonResponse.emptySuccess()
      case Some(tid) =>
        val transaction = state.transactions.get(tid)
        if (transaction == null) {
          CommonResponse.error(s"Transaction with ID=$tid is not exists", 404, TransactionNotExists)
        } else if (transaction.isCommited) {
          CommonResponse.error(s"Transaction with ID=$tid is already committed or rollback", 400, TransactionInvalid)
        } else {
          transaction.put(key, value)
          CommonResponse.emptySuccess()
        }

  final case class DeleteByKey(key: Array[Byte], tid: Option[Int],
                               replyTo: ActorRef[CommonResponse]) extends Command:
    override def doBehavior(state: InnerState): CommonResponse = tid match
      case None =>
        state.storage.delete(key)
        CommonResponse.emptySuccess()
      case Some(tid) =>
        val transaction = state.transactions.get(tid)
        if (transaction == null) {
          CommonResponse.error(s"Transaction with ID=$tid is not exists", 404, TransactionNotExists)
        } else if (transaction.isCommited) {
          CommonResponse.error(s"Transaction with ID=$tid is already committed or rollback", 400, TransactionInvalid)
        } else {
          transaction.delete(key)
          CommonResponse.emptySuccess()
        }

  final case class ScanRange(lower: Bound, upper: Bound, tid: Option[Int],
                             replyTo: ActorRef[CommonResponse]
                            ) extends Command:
    override def doBehavior(state: InnerState): CommonResponse = {
      tid match
        case None =>
          val itr = state.storage.scan(lower, upper)
          CommonResponse.success(itr.collectEntries())
        case Some(tid) =>
          val transaction = state.transactions.get(tid)
          if (transaction == null) {
            CommonResponse.error(s"Transaction with ID=$tid is not exists", 404, TransactionNotExists)
          } else if (transaction.isCommited) {
            CommonResponse.error(s"Transaction with ID=$tid is already committed or rollback", 400, TransactionInvalid)
          } else {
            val itr = transaction.scan(lower, upper)
            CommonResponse.success(itr.collectEntries())
          }
    }

  final case class State(replyTo: ActorRef[CommonResponse]) extends Command:
    override def doBehavior(state: InnerState): CommonResponse = {
      val status = state.storage.inner.dumpState()
      CommonResponse.success(status)
    }

  final case class Flush(replyTo: ActorRef[CommonResponse]) extends Command:
    override def doBehavior(state: InnerState): CommonResponse = {
      state.storage.forceFlush()
      CommonResponse.emptySuccess("Flush success")
    }

  final case class CreateTxn(replyTo: ActorRef[CommonResponse]) extends Command:
    override def doBehavior(state: InnerState): CommonResponse = {
      val txn = state.storage.newTxn()
      state.transactions.put(txn.tid, txn)
      CommonResponse.success(txn.tid)
    }

  final case class CommitTxn(tid: Int, replyTo: ActorRef[CommonResponse]) extends Command:
    override def doBehavior(state: InnerState): CommonResponse = {
      val transaction = state.transactions.remove(tid)
      if (transaction == null) {
        CommonResponse.error(s"Transaction with ID=$tid is not exists", 404, TransactionNotExists)
      } else if (transaction.isCommited) {
        CommonResponse.error(s"Transaction with ID=$tid is already committed or rollback", 400, TransactionInvalid)
      } else
        try {
          transaction.commit()
          CommonResponse.emptySuccess(s"Commit Transaction success")
        } catch
          case e: IllegalStateException => CommonResponse.error(e.getMessage, 409, TransactionCommitFailed)
          case e: Exception => CommonResponse.error("TinyLSM Server error", 500, CommonError)
    }

  final case class RollbackTxn(tid: Int, replyTo: ActorRef[CommonResponse]) extends Command:
    override def doBehavior(state: InnerState): CommonResponse = {
      val transaction = state.transactions.remove(tid)
      if (transaction == null) {
        CommonResponse.error(s"Transaction with ID=$tid is not exists", 404, TransactionNotExists)
      } else if (transaction.isCommited) {
        CommonResponse.error(s"Transaction with ID=$tid is already committed or rollback", 400, TransactionInvalid)
      } else
        try {
          transaction.rollback()
          CommonResponse.emptySuccess(s"Rollback Transaction(ID=$tid) success")
        } catch
          case e: Exception => CommonResponse.error("TinyLSM Server error", 500, CommonError)
    }
}
