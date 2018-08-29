package com.stratio.governance.agent.searcher.http

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.`Content-Type`
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import com.stratio.governance.agent.searcher.model.utils.JsonUtils
import org.json4s.DefaultFormats

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor, Future}

object HttpRequester {

  implicit val formats: DefaultFormats = org.json4s.DefaultFormats

  def partialPostRequest(json: String)(implicit system: ActorSystem): Future[HttpResponse] = {
    //http://${indexer_base_path}/domain/partial
    val entity: RequestEntity = HttpEntity(ContentType(MediaTypes.`application/json`), json)
    val request: HttpRequest =
      HttpRequest(HttpMethods.POST, uri = "http://localhost:8081/indexer/test_domain/partial", entity = entity)
    Http().singleRequest(request)
  }

  def initTotalRequest()(implicit system: ActorSystem,
                         mat: ActorMaterializer,
                         executor: ExecutionContextExecutor): String = {
    val initTotal: HttpRequest             = HttpRequest(HttpMethods.POST, uri = "http://localhost:8081/indexer/test_domain/total")
    val httpResponse: Future[HttpResponse] = Http().singleRequest(initTotal)


    val response: String = Await.result(httpResponse.flatMap { res =>
      Unmarshal(res).to[String]
    }, 10 seconds)

    println("token: " + JsonUtils.jsonStrToMap(response)("token").toString)

    JsonUtils.jsonStrToMap(response)("token").toString
  }

  def indexRequest(token: String, json: String)(implicit system: ActorSystem,
                                                mat: ActorMaterializer,
                                                executor: ExecutionContextExecutor): Future[HttpResponse] = {
    //http://localhost:8081/indexer/test_domain/total/{{token}}/index

    val putDocuments: HttpRequest = HttpRequest(
      HttpMethods.PUT,
      uri = s"http://localhost:8081/indexer/test_domain/total/$token/index",
      entity = HttpEntity(ContentTypes.`application/json`, json)
    )
    Http().singleRequest(putDocuments)
  }

  def endRequest(
    token: String
  )(implicit system: ActorSystem, mat: ActorMaterializer, executor: ExecutionContextExecutor): Future[HttpResponse] = {
    val endTotalPut: HttpRequest =
      HttpRequest(HttpMethods.PUT, uri = s"http://localhost:8081/indexer/test_domain/total/$token/end")
    Http().singleRequest(endTotalPut)
  }

}
