package com.stratio.governance.agent.searcher.actors.indexer

import akka.actor.Actor
import com.stratio.governance.agent.searcher.actors.indexer.DGIndexer.IndexerEvent
import com.stratio.governance.agent.searcher.model.es.{DatastoreEngineES, EntityRowES}
import com.stratio.governance.agent.searcher.model._
import org.json4s.native.JsonMethods.parse
import org.json4s.{DefaultFormats, JValue}
import org.postgresql.PGNotification

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class DGIndexer(params: IndexerParams) extends Actor {

  override def receive: Receive = {
    case IndexerEvent(chunk) =>

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
            params.getSourceDbo().databaseSchemaProcess(databaseSchema)
          case ("file_table") =>
            val fileTable: FileTable = json.extract[FileTable]
            params.getSourceDbo().fileTableProcess(fileTable)
          case ("file_column") =>
            val fileColumn: FileColumn = json.extract[FileColumn]
            params.getSourceDbo().fileColumnProcess(fileColumn)
          case ("sql_table") =>
            val sqlTable: SqlTable = json.extract[SqlTable]
            params.getSourceDbo().sqlTableProcess(sqlTable)
          case ("sql_column") =>
            val sqlColumn: SqlColumn = json.extract[SqlColumn]
            params.getSourceDbo().sqlColumnProcess(sqlColumn)
          case ("key_value_pair") =>
            val keyValuePair = json.extract[KeyValuePair]
            params.getSourceDbo().keyValuePairProcess(keyValuePair)
          case _ => null //TODO errors management
        }
        requestToIndexer
      }

      implicit val formats = DefaultFormats
      val documentsBulk: String = org.json4s.native.Serialization.write(lista)

      sender ! Future(params.getSearcherDbo().index(documentsBulk))

  }

}


object DGIndexer {

  /**
    * Default Actor name
    */
  lazy val NAME = "Partial Indexer"

  /**
    * Actor messages
    */
  case class IndexerEvent(notificationChunks: Array[PGNotification])

}
