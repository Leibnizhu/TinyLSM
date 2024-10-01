package io.github.leibnizhu.tinylsm.grpc

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.grpc.GrpcClientSettings
import com.google.protobuf.ByteString
import io.github.leibnizhu.tinylsm.grpc.*

import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext}

object GprcClientSample {
  implicit val sys: ActorSystem[Nothing] = ActorSystem[Nothing](Behaviors.empty[Nothing], "TinyLsmClient")
  implicit val ec: ExecutionContext = sys.executionContext
  private val grpcClient = TinyLsmRpcServiceClient(
    GrpcClientSettings.connectToServiceAt("localhost", 9526).withTls(false))

  private def getByKeyTest(): Unit = {
    val msg = Await.result(grpcClient.getKey(GetKeyRequest(ByteString.copyFromUtf8("testKey")))
      , 5.second)
    println("getKey result:" + msg.value.toStringUtf8)
    assert("testValue" == msg.value.toStringUtf8)
  }

  private def putValueTest(): Unit = {
    val reply = Await.result(grpcClient.putKey(PutKeyRequest(
      ByteString.copyFromUtf8("testKey"),
      ByteString.copyFromUtf8("testValue"))), 5.second)
    println("putKey success")
  }

  def main(args: Array[String]): Unit = {
    putValueTest()
    getByKeyTest()
    sys.terminate()
  }
}
