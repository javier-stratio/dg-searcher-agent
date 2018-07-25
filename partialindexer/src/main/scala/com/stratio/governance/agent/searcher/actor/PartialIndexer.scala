package com.stratio.governance.agent.searcher.actor

import java.sql.{Connection, ResultSet}

import akka.actor.Actor
import akka.util.Timeout
import com.stratio.governance.agent.searcher.actor.PartialIndexer.IndexerEvent
import com.stratio.governance.agent.searcher.http.HttpRequester
import com.stratio.governance.agent.searcher.model.es.{DatastoreEngineES, EntityRowES}
import com.stratio.governance.agent.searcher.model.utils.KeyValuePairMapping
import com.stratio.governance.agent.searcher.model._
import org.json4s.native.JsonMethods.parse
import org.json4s.{DefaultFormats, JValue}
import org.postgresql.PGNotification
import scalikejdbc.ConnectionPool

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.MILLISECONDS

class PartialIndexer extends Actor {

  val connection: Connection = ConnectionPool.borrow()
  implicit val formats: DefaultFormats.type = DefaultFormats
  implicit val timeout: Timeout =
    Timeout(60000, MILLISECONDS)

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

      sender ! Future(HttpRequester.partialPostRequest(documentsBulk))

  }


  def keyValuePairProcess(keyValuePair: KeyValuePair): EntityRowES = {

    val parentId = keyValuePair.parent_id
    val parentType = keyValuePair.parent_type
    val parentTable = KeyValuePairMapping.parentTable(parentType)

    val statement = connection.createStatement
    val resultSet: ResultSet = statement.executeQuery(s"select * from dg_metadata.$parentTable as parent " +
      s"join dg_metadata.key_value_pair as kvp on parent.id = kvp.parent_id and parent_type = $parentType " +
      s"where parent.id = $parentId")
    val entity: Seq[EntityRowES] = KeyValuePairMapping.entityFromResultSet(parentType, resultSet)
    resultSet.close
    statement.close

    entity.head

  }

  //TODO these function could be probably merged into a generic one, using EntityRow trait
  def databaseSchemaProcess(databaseSchema: DatabaseSchema): EntityRowES = {

    val parentId = databaseSchema.id
    val parentType = KeyValuePairMapping.parentType(DatabaseSchema.entity)

    val statement = connection.createStatement
    val resultSet: ResultSet = statement.executeQuery(s"select * from dg_metadata.key_value_pair " +
      s"where parent_id = $parentId and parent_type = $parentType")

    val entity: Seq[EntityRowES] = DatabaseSchema.entityFromResultSet(databaseSchema, resultSet)
    resultSet.close
    statement.close

    entity.head

  }

  def fileTableProcess(fileTable: FileTable): EntityRowES = {

    val parentId = fileTable.id
    val parentType = KeyValuePairMapping.parentType(FileTable.entity)

    val statement = connection.createStatement
    val resultSet: ResultSet = statement.executeQuery(s"select * from dg_metadata.key_value_pair " +
      s"where parent_id = $parentId and parent_type = $parentType")

    val entity: Seq[EntityRowES] = FileTable.entityFromResultSet(fileTable, resultSet)
    resultSet.close
    statement.close

    entity.head
  }

  def fileColumnProcess(fileColumn: FileColumn): EntityRowES = {

    val parentId = fileColumn.id
    val parentType = KeyValuePairMapping.parentType(FileColumn.entity)

    val statement = connection.createStatement
    val resultSet: ResultSet = statement.executeQuery(s"select * from dg_metadata.key_value_pair " +
      s"where parent_id = $parentId and parent_type = $parentType")

    val entity: Seq[EntityRowES] = FileColumn.entityFromResultSet(fileColumn, resultSet)
    resultSet.close
    statement.close

    entity.head
  }

  def sqlTableProcess(sqlTable: SqlTable): EntityRowES = {

    val parentId = sqlTable.id
    val parentType = KeyValuePairMapping.parentType(SqlTable.entity)

    val statement = connection.createStatement
    val resultSet: ResultSet = statement.executeQuery(s"select * from dg_metadata.key_value_pair " +
      s"where parent_id = $parentId and parent_type = $parentType")

    val entity: Seq[EntityRowES] = SqlTable.entityFromResultSet(sqlTable, resultSet)
    resultSet.close
    statement.close

    entity.head
  }

  def sqlColumnProcess(sqlColumn: SqlColumn): EntityRowES = {

    val parentId = sqlColumn.id
    val parentType = KeyValuePairMapping.parentType(SqlColumn.entity)

    val statement = connection.createStatement
    val resultSet: ResultSet = statement.executeQuery(s"select * from dg_metadata.key_value_pair " +
      s"where parent_id = $parentId and parent_type = $parentType")

    val entity: Seq[EntityRowES] = SqlColumn.entityFromResultSet(sqlColumn, resultSet)
    resultSet.close
    statement.close

    entity.head
  }

}


object PartialIndexer {

  /**
    * Default Actor name
    */
  lazy val NAME = "Partial Indexer"

  /**
    * Actor messages
    */
  case class IndexerEvent(notificationChunks: Array[PGNotification])

}
