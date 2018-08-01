package com.stratio.governance.agent.searcher.totalindex.actor

import java.sql.Connection

import akka.actor.Actor
import akka.util.Timeout
import com.stratio.governance.agent.searcher.http.HttpRequester
import com.stratio.governance.agent.searcher.model.DatastoreEngine
import com.stratio.governance.agent.searcher.model.es.DatastoreEngineES
import com.stratio.governance.agent.searcher.totalindex.actor.TotalIndexer.IndexerEvent
import org.json4s.DefaultFormats
import scalikejdbc.ConnectionPool

import scala.concurrent.Future
import scala.concurrent.duration.MILLISECONDS
import scala.concurrent.ExecutionContext.Implicits.global

class TotalIndexer extends Actor {

  implicit val formats: DefaultFormats.type = DefaultFormats

  val connection: Connection = ConnectionPool.borrow()
  implicit val timeout: Timeout =
    Timeout(60000, MILLISECONDS)

  override def receive = {
    case IndexerEvent(chunk) =>
      val list = chunk.map { entity =>
        DatastoreEngineES.fromDatastoreEngine(entity)
      }

      val documentsBulk: String = org.json4s.native.Serialization.write(list)

//      sender ! Future(HttpRequester.totalPostRequest("", documentsBulk))

    //TODO call total indexer endpoint and control status and errors
    /*
      val lista: Array[EntityRowES] = chunk.map { not =>

        // convert to object
        implicit val formats: DefaultFormats.type = DefaultFormats
        val json: JValue = parse(not.getParameter) \\ "data"
        val table: JValue = parse(not.getParameter) \\ "table"

        //TODO check JSONObject can parse JsonB postgresql type

        val requestToIndexer: EntityRowES = table.values match {
          case ("datastore_engine") =>
            val datastoreEngine: DatastoreEngine = json.extract[DatastoreEngine]
            val datastoreEngineES = Seq(DatastoreEngineES.fromDatastoreEngine(datastoreEngine))
            datastoreEngineES.head
          case ("database_schema") =>
            val databaseSchema: DatabaseSchema = json.extract[DatabaseSchema]
            databaseSchemaProcess(databaseSchema)
          case ("file_table") =>
            val fileTable: FileTable = json.extract[FileTable]
            fileTableProcess(fileTable)
          case ("file_column") =>
            val fileColumn: FileColumn = json.extract[FileColumn]
            fileColumnProcess(fileColumn)
          case ("sql_table") =>
            val sqlTable: SqlTable = json.extract[SqlTable]
            sqlTableProcess(sqlTable)
          case ("sql_column") =>
            val sqlColumn: SqlColumn = json.extract[SqlColumn]
            sqlColumnProcess(sqlColumn)
          case ("key_value_pair") =>
            val keyValuePair = json.extract[KeyValuePair]
            keyValuePairProcess(keyValuePair)
          case _ => null //TODO errors management
        }
        requestToIndexer
      }

      val documentsBulk: String = org.json4s.native.Serialization.write(lista)

      sender ! Future(HttpRequester.postRequest(documentsBulk))
   */
  }
}

object TotalIndexer {

  /**
   * Default Actor name
   */
  lazy val NAME = "Total Indexer"

  /**
   * Actor messages
   */
  case class IndexerEvent(notificationChunks: Seq[DatastoreEngine])

}
