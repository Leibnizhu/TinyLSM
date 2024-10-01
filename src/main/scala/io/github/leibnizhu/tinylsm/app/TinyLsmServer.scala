package io.github.leibnizhu.tinylsm.app

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.{HttpRequest, HttpResponse}
import org.apache.pekko.http.scaladsl.server.Route
import com.typesafe.config.ConfigFactory
import io.github.leibnizhu.tinylsm.grpc.TinyLsmRpcServiceHandler
import io.github.leibnizhu.tinylsm.mvcc.Transaction
import io.github.leibnizhu.tinylsm.utils.Config
import io.github.leibnizhu.tinylsm.{LsmStorageOptions, TinyLsm}
import org.slf4j.LoggerFactory

import java.io.File
import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class TinyLsmServer(storage: TinyLsm, host: String, httpPort: Int, rpcPort: Int) {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private var actorSys: ActorSystem[Nothing] = _
  Runtime.getRuntime.addShutdownHook(new Thread(() => {
    logger.info("Start to close TinyLSM...")
    storage.close()
    if (actorSys != null) {
      actorSys.terminate()
    }
    logger.info("Finish closing TinyLSM...")
  }))
  private val transactions = new ConcurrentHashMap[Int, Transaction]()

  def start(): Unit = {
    //#server-bootstrapping
    val rootBehavior = Behaviors.setup[Nothing] { context =>
      val httpRegistry = TinyLsmHttpRegistry(storage, transactions).registry()
      val tinyLsmActor = context.spawn(httpRegistry, "TinyLsmActor")
      context.watch(tinyLsmActor)
      val routes = new HttpRoutes(tinyLsmActor)(context.system)
      startHttpServer(routes.routes)(context.system)
      startRpcServer()(context.system)
      Behaviors.empty
    }
    val conf = ConfigFactory.parseString("pekko.http.server.enable-http2=on")
      .withFallback(ConfigFactory.defaultApplication())
    actorSys = ActorSystem[Nothing](rootBehavior, "TinyLsmAkkaServer", conf)
    logger.info("===> TinyLSM Server Started")
  }

  private def startHttpServer(routes: Route)(implicit system: ActorSystem[_]): Unit = {
    // Akka HTTP still needs a classic ActorSystem to start
    import system.executionContext

    val futureBinding = Http().newServerAt(host, httpPort).bind(routes)
    futureBinding.onComplete {
      case Success(binding) =>
        val address = binding.localAddress
        logger.info("TinyLSM HTTP Server online at http://{}:{}/", address.getHostString, address.getPort)
      case Failure(ex) =>
        logger.error("Failed to bind TinyLSM HTTP endpoint, terminating system", ex)
        system.terminate()
    }
  }

  private def startRpcServer()(implicit system: ActorSystem[_]): Unit = {
    implicit val ec: ExecutionContext = system.executionContext

    val service: HttpRequest => Future[HttpResponse] =
      TinyLsmRpcServiceHandler(GrpcServiceImpl(storage, transactions, system))

    val bound: Future[Http.ServerBinding] = Http()
      .newServerAt(host, rpcPort)
      .bind(service)
      .map(_.addToCoordinatedShutdown(hardTerminationDeadline = 10.seconds))

    bound.onComplete {
      case Success(binding) =>
        val address = binding.localAddress
        logger.info("TinyLSM gRPC server online at {}:{}", address.getHostString, address.getPort)
      case Failure(ex) =>
        logger.info("Failed to bind TinyLSM gRPC endpoint, terminating system")
        ex.printStackTrace()
        system.terminate()
    }
  }
}

object TinyLsmServer {
  def main(args: Array[String]): Unit = {
    Config.print()
    TinyLsmServer().start()
  }

  def apply(): TinyLsmServer = {
    val httpPort = Config.HttpPort.getInt
    val rpcPort = Config.GrpcPort.getInt
    val host = Config.Host.get()
    val lsmOptions = LsmStorageOptions.fromConfig()
    val dataDir = new File(Config.DataDir.get())
    val storage = TinyLsm(dataDir, lsmOptions)
    new TinyLsmServer(storage, host, httpPort, rpcPort)
  }
}
