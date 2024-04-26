package io.github.leibnizhu.tinylsm.app

import akka.actor.typed.ActorSystem
import io.github.leibnizhu.tinylsm.app.BizCode.{Success, TransactionInvalid, TransactionNotExists}
import io.github.leibnizhu.tinylsm.app.TinyLsmHttpRegistry.*
import io.github.leibnizhu.tinylsm.grpc.*
import io.github.leibnizhu.tinylsm.iterator.StorageIterator
import io.github.leibnizhu.tinylsm.mvcc.Transaction
import io.github.leibnizhu.tinylsm.utils.Bound
import io.github.leibnizhu.tinylsm.{RawKey, TinyLsm}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future

class TinyLsmRpcServiceImpl(state: TinyLsmHttpRegistry.InnerState,
                            system: ActorSystem[_]) extends TinyLsmRpcService {

  override def getKey(in: GetKeyRequest): Future[ValueReply] = {
    val response = GetByKey(in.key, in.tid, null).wrapBehavior(state)
    Future.successful(response.bizCode match
      case Success => ValueReply.of(response.content, response.bizCode.code, None)
      case _ => ValueReply.of("", response.bizCode.code, Some(response.content))
    )
  }

  override def putKey(in: PutKeyRequest): Future[EmptySuccessReply] = {
    val response = PutValue(in.key, in.value, in.tid, null).wrapBehavior(state)
    Future.successful(toEmptyReply(response))
  }

  override def deleteKey(in: DeleteKeyRequest): Future[EmptySuccessReply] = {
    val response = DeleteByKey(in.key, in.tid, null).wrapBehavior(state)
    Future.successful(toEmptyReply(response))
  }

  override def scan(in: ScanRequest): Future[ScanReply] = {
    val lower = Bound(in.fromType.name, in.fromKey)
    val upper = Bound(in.toType.name, in.toKey)

    def iteratorToScanReply(itr: StorageIterator[RawKey]) = {
      val buffer = new ArrayBuffer[ScanReply.KeyValue]()
      while (itr.isValid) {
        buffer += ScanReply.KeyValue.of(new String(itr.key().bytes), new String(itr.value()))
        itr.next()
      }
      ScanReply.of(buffer.toArray, Success.code, None)
    }

    val reply = in.tid match
      case None =>
        val itr = state.storage.scan(lower, upper)
        iteratorToScanReply(itr)
      case Some(tid) =>
        val transaction = state.transactions.get(tid)
        if (transaction == null) {
          val errMsg = s"Transaction with ID=$tid is not exists"
          ScanReply.of(List(), TransactionNotExists.code, Some(errMsg))
        } else if (transaction.isCommited) {
          val errMsg = s"Transaction with ID=$tid is already committed or rollback"
          ScanReply.of(List(), TransactionInvalid.code, Some(errMsg))
        } else {
          val itr = transaction.scan(lower, upper)
          iteratorToScanReply(itr)
        }
    Future.successful(reply)
  }

  override def dumpState(in: Empty): Future[StateReply] = {
    val response = State(null).wrapBehavior(state)
    Future.successful(response.bizCode match
      case Success => StateReply.of(response.content, response.bizCode.code, None)
      case _ => StateReply.of("", response.bizCode.code, Some(response.content))
    )
  }

  override def forceFlush(in: Empty): Future[EmptySuccessReply] = {
    val response = Flush(null).wrapBehavior(state)
    Future.successful(toEmptyReply(response))
  }

  override def createTxn(in: Empty): Future[NewTxnReply] = {
    val response = CreateTxn(null).wrapBehavior(state)
    Future.successful(response.bizCode match
      case Success => NewTxnReply.of(response.content.toInt, response.bizCode.code, None)
      case _ => NewTxnReply.of(-1, response.bizCode.code, Some(response.content))
    )
  }

  override def commitTxn(in: TxnRequest): Future[EmptySuccessReply] = {
    val response = CommitTxn(in.tid, null).wrapBehavior(state)
    Future.successful(toEmptyReply(response))
  }

  override def rollbackTxn(in: TxnRequest): Future[EmptySuccessReply] = {
    val response = RollbackTxn(in.tid, null).wrapBehavior(state)
    Future.successful(toEmptyReply(response))
  }

  private def toEmptyReply(response: TinyLsmHttpRegistry.CommonResponse) = {
    EmptySuccessReply(response.bizCode.code, Option(response.content))
  }
}

object TinyLsmRpcServiceImpl {
  def apply(storage: TinyLsm, transactions: java.util.Map[Int, Transaction], system: ActorSystem[_]): TinyLsmRpcServiceImpl =
    new TinyLsmRpcServiceImpl(TinyLsmHttpRegistry.InnerState(storage, transactions), system)
}