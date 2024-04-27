package io.github.leibnizhu.tinylsm.app

import akka.actor.typed.ActorSystem
import com.google.protobuf.ByteString
import io.github.leibnizhu.tinylsm.app.ApiCommands.*
import io.github.leibnizhu.tinylsm.app.BizCode.{Success, TransactionInvalid, TransactionNotExists}
import io.github.leibnizhu.tinylsm.grpc.*
import io.github.leibnizhu.tinylsm.iterator.StorageIterator
import io.github.leibnizhu.tinylsm.mvcc.Transaction
import io.github.leibnizhu.tinylsm.utils.Bound
import io.github.leibnizhu.tinylsm.{Key, MemTableValue, RawKey, TinyLsm}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future

class TinyLsmRpcServiceImpl(state: ApiCommands.InnerState,
                            system: ActorSystem[_]) extends TinyLsmRpcService {

  override def getKey(in: GetKeyRequest): Future[ValueReply] = {
    val response = GetByKey(in.key.toByteArray, in.tid, null).wrapBehavior(state)
    Future.successful(response.bizCode match
      case Success =>
        val value = ByteString.copyFrom(response.data.asInstanceOf[MemTableValue])
        ValueReply.of(value, response.bizCode.code, None)
      case _ => ValueReply.of(ByteString.empty(), response.bizCode.code, Some(response.message))
    )
  }

  override def putKey(in: PutKeyRequest): Future[EmptySuccessReply] = {
    val response = PutValue(in.key.toByteArray, in.value.toByteArray, in.tid, null).wrapBehavior(state)
    Future.successful(toEmptyReply(response))
  }

  override def deleteKey(in: DeleteKeyRequest): Future[EmptySuccessReply] = {
    val response = DeleteByKey(in.key.toByteArray, in.tid, null).wrapBehavior(state)
    Future.successful(toEmptyReply(response))
  }

  override def scan(in: ScanRequest): Future[ScanReply] = {
    val lower = Bound(in.fromType.name, in.fromKey.toByteArray)
    val upper = Bound(in.toType.name, in.toKey.toByteArray)
    val response = ScanRange(lower, upper, in.tid, null).wrapBehavior(state)
    val reply = response.bizCode match
      case Success =>
        val entries = response.data.asInstanceOf[List[(Key, MemTableValue)]].map(entry => ScanReply.KeyValue.of(
          ByteString.copyFrom(entry._1.bytes), ByteString.copyFrom(entry._2)
        ))
        ScanReply.of(entries, response.bizCode.code, None)
      case _ => ScanReply.of(List(), response.bizCode.code, Some(response.message))
    Future.successful(reply)
  }

  override def dumpState(in: Empty): Future[StateReply] = {
    val response = State(null).wrapBehavior(state)
    Future.successful(response.bizCode match
      case Success => StateReply.of(response.data.asInstanceOf[String], response.bizCode.code, None)
      case _ => StateReply.of("", response.bizCode.code, Some(response.message))
    )
  }

  override def forceFlush(in: Empty): Future[EmptySuccessReply] = {
    val response = Flush(null).wrapBehavior(state)
    Future.successful(toEmptyReply(response))
  }

  override def createTxn(in: Empty): Future[NewTxnReply] = {
    val response = CreateTxn(null).wrapBehavior(state)
    Future.successful(response.bizCode match
      case Success => NewTxnReply.of(response.data.asInstanceOf[Int], response.bizCode.code, None)
      case _ => NewTxnReply.of(-1, response.bizCode.code, Some(response.message))
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

  private def toEmptyReply(response: ApiCommands.CommonResponse) = {
    EmptySuccessReply(response.bizCode.code, Option(response.message))
  }
}

object TinyLsmRpcServiceImpl {
  def apply(storage: TinyLsm, transactions: java.util.Map[Int, Transaction], system: ActorSystem[_]): TinyLsmRpcServiceImpl =
    new TinyLsmRpcServiceImpl(ApiCommands.InnerState(storage, transactions), system)
}