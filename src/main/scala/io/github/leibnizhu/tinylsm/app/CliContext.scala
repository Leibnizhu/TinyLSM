package io.github.leibnizhu.tinylsm.app

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.grpc.GrpcClientSettings
import com.google.protobuf.ByteString
import io.github.leibnizhu.tinylsm.grpc.*
import io.github.leibnizhu.tinylsm.grpc.ScanRequest.BoundType
import io.github.leibnizhu.tinylsm.mvcc.Transaction
import io.github.leibnizhu.tinylsm.utils.{Bound, Config}
import io.github.leibnizhu.tinylsm.{LsmStorageInner, LsmStorageOptions}

import java.io.File
import java.util.StringJoiner
import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success}


class CliContext(playgroundMode: Boolean,
                 playgroundLsm: Option[LsmStorageInner],
                 debugMode: Boolean,
                 host: String,
                 port: Int) {
  // for playground 模式
  private[app] var currentTxn: Option[Transaction] = None
  // for 连接服务器模式，非 playground
  private[app] var currentTxnId: Option[Int] = None

  implicit val sys: ActorSystem[Nothing] = ActorSystem[Nothing](Behaviors.empty[Nothing], "TinyLsmClient")
  implicit val ec: ExecutionContext = sys.executionContext
  private val grpcClient = TinyLsmRpcServiceClient(GrpcClientSettings.connectToServiceAt(host, port).withTls(false))

  Runtime.getRuntime.addShutdownHook(new Thread(() => {
    rollbackCurrentTxn()
    sys.terminate()
  }))

  def cliPrompt(): String = if (playgroundMode) {
    currentTxn match
      case Some(txn) => s"TinyLsm(Txn:${txn.tid})> "
      case None => "TinyLsm> "
  } else {
    currentTxnId match
      case Some(tid) => s"TinyLsm(Txn:$tid)> "
      case None => "TinyLsm> "
  }

  def get(key: String): Unit =
    if (playgroundMode) {
      val value = currentTxn match
        case Some(txn) => txn.get(key)
        case None => playgroundLsm.get.get(key)
      if (value.isDefined) {
        println(value.get)
      } else {
        println(">>> Key does not exists: " + key)
      }
    } else {
      val reply = grpcClient.getKey(GetKeyRequest(ByteString.copyFromUtf8(key), currentTxnId))
      handleCommonReply(reply, _.bizCode, _.message, msg => println(msg.value.toStringUtf8))
    }

  def delete(key: String): Unit =
    if (playgroundMode) {
      currentTxn match
        case Some(txn) => txn.delete(key)
        case None => playgroundLsm.get.delete(key)
      println("Done")
    } else {
      val reply = grpcClient.deleteKey(DeleteKeyRequest(ByteString.copyFromUtf8(key), currentTxnId))
      handleEmptyReply(reply, "Delete success")
    }

  def put(key: String, value: String): Unit =
    if (playgroundMode) {
      currentTxn match
        case Some(txn) => txn.put(key, value)
        case None => playgroundLsm.get.put(key, value)
      println("Done")
    } else {
      val reply = grpcClient.putKey(PutKeyRequest(ByteString.copyFromUtf8(key), ByteString.copyFromUtf8(value), currentTxnId))
      handleEmptyReply(reply, "Put value success")
    }

  private val validBoundType = Set("unbounded", "excluded", "included")

  def scan(fromType: String, fromKey: String, toType: String, toKey: String): Unit = {
    if (!validBoundType.contains(fromType.toLowerCase)) {
      println(">>> Invalid command, fromType must be one of: " + validBoundType)
      return
    }
    if (!validBoundType.contains(toType.toLowerCase)) {
      println(">>> Invalid command, toType must be one of: " + validBoundType)
      return
    }

    if (playgroundMode) {
      val lower = Bound(fromType, fromKey)
      val upper = Bound(toType, toKey)
      val itr = currentTxn match
        case Some(txn) => txn.scan(lower, upper)
        case None => playgroundLsm.get.scan(lower, upper)
      val sj = new StringJoiner("\n")
      println(itr.joinAllKeyValue(sj).toString)
    } else {
      def toBoundType(t: String): BoundType = t.toLowerCase match
        case "unbounded" => BoundType.UNBOUNDED
        case "excluded" => BoundType.EXCLUDED
        case "included" => BoundType.INCLUDED

      val reply = grpcClient.scan(ScanRequest(
        toBoundType(fromType), ByteString.copyFromUtf8(fromKey),
        toBoundType(toType), ByteString.copyFromUtf8(toKey), currentTxnId))
      handleCommonReply(reply, _.bizCode, _.message, msg => for (kv <- msg.kvs) {
        println(new String(kv.key.toByteArray))
        println(new String(kv.value.toByteArray))
      })
    }
  }

  def flush(): Unit = {
    if (!debugMode) {
      println(">>> flush command can only be used in debug mode!")
      return
    }
    if (playgroundMode) {
      playgroundLsm.get.forceFreezeMemTable()
      playgroundLsm.get.forceFlushNextImmutableMemTable()
    } else {
      val reply = grpcClient.forceFlush(Empty())
      handleEmptyReply(reply, "Flush success")
    }
  }

  def status(): Unit = {
    if (!debugMode) {
      println(">>> status command can only be used in debug mode!")
      return
    }
    if (playgroundMode) {
      playgroundLsm.get.dumpState()
    } else {
      val reply = grpcClient.dumpState(Empty())
      handleCommonReply(reply, _.bizCode, _.message, msg => println(msg.state))
    }
  }

  def newTxn(): Unit =
    if (playgroundMode) {
      currentTxn match
        case Some(txn) => println(s">>> Already start a Transaction, ID= ${txn.tid}")
        case None =>
          currentTxn = Some(playgroundLsm.get.newTxn())
          println(s"Start a new Transaction, ID: ${currentTxn.get.tid}")
    } else {
      currentTxnId match
        case Some(tid) => println(s">>> Already start a Transaction, ID= $tid")
        case None =>
          val reply = grpcClient.createTxn(Empty())
          handleCommonReply(reply, _.bizCode, _.message, msg => {
            currentTxnId = Some(msg.tid)
            println(s">>> Start a new Transaction, ID: ${msg.tid}")
          })
    }

  def commitCurrentTxn(): Unit =
    if (playgroundMode) {
      currentTxn match
        case Some(txn) =>
          txn.commit()
          currentTxn = None
          println(s">>> Committed Transaction, ID: ${txn.tid}")
        case None => println(">>> No active Transaction!")
    } else {
      currentTxnId match
        case Some(tid) =>
          val reply = grpcClient.commitTxn(TxnRequest(tid))
          handleCommonReply(reply, _.bizCode, _.message, msg => {
            currentTxnId = None
            println(s">>> Committed Transaction, ID: $tid")
          })
        case None => println(">>> No active Transaction!")
    }

  def rollbackCurrentTxn(): Unit =
    if (playgroundMode) {
      currentTxn match
        case Some(txn) =>
          txn.rollback()
          currentTxn = None
          println(s">>> Rollback Transaction, ID: ${txn.tid}")
        case None => println(">>> No active Transaction!")
    } else {
      currentTxnId match
        case Some(tid) =>
          val reply = grpcClient.rollbackTxn(TxnRequest(tid))
          handleCommonReply(reply, _.bizCode, _.message, msg => {
            currentTxnId = None
            println(s">>> Rollback Transaction, ID: $tid")
          })
        case None => println(">>> No active Transaction!")
    }

  private def handleEmptyReply(reply: Future[EmptySuccessReply], info: String): Unit =
    handleCommonReply(reply, _.bizCode, _.message, _ => println(info))

  private def handleCommonReply[R](reply: Future[R],
                                   getCode: R => Int, getMessage: R => Option[String], action: R => Unit): Unit = {
    reply onComplete {
      case Success(msg) => getCode(msg) match
        case BizCode.Success.code => action(msg)
        case code: Int => println(">>> " + getMessage(msg).getOrElse(unknownError(code)))
      case Failure(e) =>
        println(s">>> Server Error: $e")
    }
    Await.result(reply, 5.second)
  }

  private def unknownError(bizCode: Int): String = s"Unknown error, code: $bizCode"
}

object CliContext {
  def apply(argMap: Map[String, Any]): CliContext = {
    val playgroundMode = argMap.getOrElse("playground", false).asInstanceOf[Boolean]
    val debugMode = argMap.getOrElse("debug", false).asInstanceOf[Boolean]
    val playgroundLsm = if (playgroundMode) {
      val tempDir = new File(System.getProperty("java.io.tmpdir") + File.separator + "TinyLsmPlayground")
      if (tempDir.exists()) {
        tempDir.delete()
      }
      tempDir.mkdirs()
      println(s"Start a playground TinyLSM instance...")
      Some(LsmStorageInner(tempDir, LsmStorageOptions.defaultOption()))
    } else {
      None
    }
    new CliContext(
      playgroundMode,
      playgroundLsm,
      debugMode,
      argMap.getOrElse("host", "localhost").asInstanceOf[String],
      argMap.getOrElse("port", Config.GrpcPort.defaultVal.toInt).asInstanceOf[Int]
    )
  }
}