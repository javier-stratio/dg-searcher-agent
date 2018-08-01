package com.stratio.governance.agent.searcher.totalindex.main

import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpResponse
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.stratio.governance.agent.searcher.http.HttpRequester
import com.stratio.governance.agent.searcher.http.HttpRequester._
import com.stratio.governance.agent.searcher.model.DatastoreEngine
import com.stratio.governance.agent.searcher.model.es.DatastoreEngineES
import org.json4s.DefaultFormats
import scalikejdbc._
import scalikejdbc.streams._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object TotalIndexer {

  implicit val formats: DefaultFormats.type = DefaultFormats

  def indexAll()(implicit materializer: ActorMaterializer, actorSystem: ActorSystem) = {

    val token: String = initTotalRequest()

    val publisher: DatabasePublisher[DatastoreEngine] = DB.readOnlyStream {
      sql"SELECT * FROM dg_metadata.datastore_engine"
        .map(r => DatastoreEngine.getOneResult(r.underlying))
        .iterator
        .withDBSessionForceAdjuster(session => {
          session.connection.setAutoCommit(false)
          session.fetchSize(10)
        })
    }

    Source
      .fromPublisher(publisher)
      .map(DatastoreEngineES.fromDatastoreEngine)
      .grouped(1000)
      .mapAsync(1){x => println(x.head);HttpRequester.indexRequest(token, org.json4s.native.Serialization.write(x))}
//      .foldAsync(List[HttpResponse]()){case (acc, fut) => acc.}
//      .mapAsync(10)(identity)
      .map{x => println(x); x}
      .to(Sink.ignore)
      .run


//    endRequest(token)
  }

}
