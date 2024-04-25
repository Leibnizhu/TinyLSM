package io.github.leibnizhu.tinylsm.app

import akka.actor.typed.scaladsl.AskPattern.*
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.http.scaladsl.server.Directives.*
import akka.http.scaladsl.server.Route
import akka.util.Timeout
import io.github.leibnizhu.tinylsm.app.TinyLsmHttpRegistry.{CommonResponse, DeleteByKey, GetByKey, PutValue}

import java.time.Duration
import scala.concurrent.Future

class TinyLsmHttpRoutes(registry: ActorRef[TinyLsmHttpRegistry.Command])
                       (implicit val system: ActorSystem[_]) {

  //#user-routes-class

  // If ask takes more time than this to complete the request is failed
  private implicit val timeout: Timeout = Timeout.create(Duration.ofSeconds(10))


  private def getByKey(key: String, tid: Option[Int]): Future[CommonResponse] =
    registry.ask(GetByKey(key, tid, _))

  private def deleteByKey(key: String, tid: Option[Int]): Future[CommonResponse] =
    registry.ask(DeleteByKey(key, tid, _))

  private def putValue(key: String, value: String, tid: Option[Int]): Future[CommonResponse] =
    registry.ask(PutValue(key, value, tid, _))

  val routes: Route =
    pathPrefix("key") {
      concat(
        // key get/put/delete starts
        path(Segment) { key =>
          parameter(Symbol("tid").as[Int].?) { tid =>
            concat(
              get {
                onSuccess(getByKey(key, tid)) { response =>
                  complete(response.status, response.content)
                }
              },
              post {
                parameter(Symbol("value")) { value =>
                  onSuccess(putValue(key, value, tid)) { response =>
                    complete(response.status, response.content)
                  }
                }
              },
              delete {
                onSuccess(deleteByKey(key, tid)) { response =>
                  complete(response.status, response.content)
                }
              }
            )
          }
        }
      )
      // key get/put/delete ends
    }
}
