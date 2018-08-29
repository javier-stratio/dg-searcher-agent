package com.stratio.governance.agent.searcher.totalindex.main

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpResponse
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import com.stratio.governance.agent.searcher.http.HttpRequester
import com.stratio.governance.agent.searcher.http.HttpRequester._
import com.stratio.governance.agent.searcher.model.DatastoreEngine
import com.stratio.governance.agent.searcher.model.es.{DatastoreEngineES, EntityRowES}
import org.json4s.DefaultFormats
import scalikejdbc._
import scalikejdbc.streams._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object TotalIndexer {

  implicit val formats: DefaultFormats.type = DefaultFormats

  def generateSource[T](dbPublisher: DatabasePublisher[T], toESModel: T => EntityRowES)(
    implicit system: ActorSystem,
    materializer: ActorMaterializer
  ): Source[EntityRowES, NotUsed] = {
    Source
      .fromPublisher(dbPublisher)
      .map(toESModel)
  }

  def sink(token: String)(
    implicit system: ActorSystem,
    materializer: ActorMaterializer
  ): Sink[EntityRowES, Future[Done]] = {
    Flow[EntityRowES]
      .grouped(1000)
      .mapAsync(1) { x =>
        HttpRequester.indexRequest(token, org.json4s.native.Serialization.write(x))
      }
      .map { x =>
        x.entity.discardBytes()
        x.status
      }
      .toMat(Sink.ignore)(Keep.right)
  }

  def indexAll()(implicit materializer: ActorMaterializer, actorSystem: ActorSystem) = {

    //TODO rgarcia: must be optional due to map accessing
    val token: String = initTotalRequest()

    println(System.currentTimeMillis())

    generateSource(DatastoreEngine.publisher, DatastoreEngineES.fromDatastoreEngine)
      .runWith(sink(token))
      .onComplete(_ => println(System.currentTimeMillis()))
//    endRequest(token)
  }

  def goingIndexing(token: String)(implicit system: ActorSystem, materializer: ActorMaterializer) = {
    generateSource(DatastoreEngine.publisher, DatastoreEngineES.fromDatastoreEngine)
  }

}
