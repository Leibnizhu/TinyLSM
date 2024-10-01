package io.github.leibnizhu.tinylsm.app

import org.apache.pekko.actor.typed.scaladsl.AskPattern.*
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem}
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.util.Timeout
import io.github.leibnizhu.tinylsm.app.ApiCommands.*
import io.github.leibnizhu.tinylsm.utils.Bound

import java.time.Duration
import scala.concurrent.Future

class HttpRoutes(registry: ActorRef[ApiCommands.Command])
                (implicit val system: ActorSystem[_]) {

  //#user-routes-class

  // If ask takes more time than this to complete the request is failed
  private implicit val timeout: Timeout = Timeout.create(Duration.ofSeconds(10))


  private def getByKey(key: String, tid: Option[Int]): Future[CommonResponse] =
    registry.ask(GetByKey(key.getBytes, tid, _))

  private def deleteByKey(key: String, tid: Option[Int]): Future[CommonResponse] =
    registry.ask(DeleteByKey(key.getBytes, tid, _))

  private def putValue(key: String, value: String, tid: Option[Int]): Future[CommonResponse] =
    registry.ask(PutValue(key.getBytes, value.getBytes, tid, _))

  private def scan(fromType: String, fromKey: String, toType: String, toKey: String,
                   tid: Option[Int] = None): Future[CommonResponse] = {
    val lower = Bound(fromType, fromKey)
    val upper = Bound(toType, toKey)
    registry.ask(ScanRange(lower, upper, tid, _))
  }

  private def dumpState(): Future[CommonResponse] = registry.ask(State.apply)

  private def forceFlush(): Future[CommonResponse] = registry.ask(Flush.apply)

  private def newTransaction(): Future[CommonResponse] = registry.ask(CreateTxn.apply)

  private def commitTransaction(tid: Int): Future[CommonResponse] = registry.ask(CommitTxn(tid, _))

  private def rollbackTransaction(tid: Int): Future[CommonResponse] = registry.ask(RollbackTxn(tid, _))

  val routes: Route =
    concat(
      // key api get/put/delete starts
      pathPrefix("key") {
        path(Segment) { key =>
          parameter(Symbol("tid").as[Int].?) { tid =>
            get {
              onSuccess(getByKey(key, tid))(_.response())
            } ~
              post {
                parameter(Symbol("value")) { value =>
                  onSuccess(putValue(key, value, tid))(_.response())
                }
              } ~
              delete {
                onSuccess(deleteByKey(key, tid))(_.response())
              }
          }
        }
        // key api get/put/delete ends
      },
      // scan api starts
      path("scan") {
        get {
          parameter(Symbol("fromType"), Symbol("fromKey"), Symbol("toType"), Symbol("toKey"), Symbol("tid").as[Int].?) {
            (fromType, fromKey, toType, toKey, tid) =>
              onSuccess(scan(fromType, fromKey, toType, toKey, tid))(_.response())
          }
        }
        // scan api ends
      },
      // system api starts
      pathPrefix("sys") {
        post {
          path("flush") {
            onSuccess(forceFlush())(_.response())
          } ~
            path("state") {
              onSuccess(dumpState())(_.response())
            }
        }
        // system api ends
      },
      // transaction api starts
      post {
        path("txn") {
          onSuccess(newTransaction())(_.response())
        } ~
          pathPrefix("txn" / IntNumber) { tid =>
            path("commit") {
              onSuccess(commitTransaction(tid))(_.response())
            } ~
              path("rollback") {
                onSuccess(rollbackTransaction(tid))(_.response())
              }
          }
        // transaction api ends
      }
    )
}
