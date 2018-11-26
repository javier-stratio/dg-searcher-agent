package com.stratio.governance.agent.searcher.actors.indexer

import java.sql.{Connection, ResultSet}

import akka.util.Timeout
import com.stratio.governance.agent.searcher.actors.indexer.dao.{SearcherDao, SourceDao}
import com.stratio.governance.agent.searcher.http.HttpRequester
import com.stratio.governance.agent.searcher.model._
import com.stratio.governance.agent.searcher.model.es.EntityRowES
import com.stratio.governance.agent.searcher.model.utils.KeyValuePairMapping
import org.json4s.DefaultFormats
import scalikejdbc.ConnectionPool

import scala.concurrent.duration.MILLISECONDS

class DGIndexerParams extends IndexerParams {

  val sourceDbo: SourceDao = new SourceDao {

    val connection: Connection = ConnectionPool.borrow()
    implicit val formats: DefaultFormats.type = DefaultFormats
    implicit val timeout: Timeout =
      Timeout(60000, MILLISECONDS)

    override def keyValuePairProcess(keyValuePair: KeyValuePair): EntityRowES = {

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
    override def databaseSchemaProcess(databaseSchema: DatabaseSchema): EntityRowES = {

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

    override def fileTableProcess(fileTable: FileTable): EntityRowES = {

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

    override def fileColumnProcess(fileColumn: FileColumn): EntityRowES = {

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

    override def sqlTableProcess(sqlTable: SqlTable): EntityRowES = {

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

    override def sqlColumnProcess(sqlColumn: SqlColumn): EntityRowES = {

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

  val searcherDbo: SearcherDao = new SearcherDao {
    override def index(doc: String): Unit = {
      HttpRequester.partialPostRequest(doc)
    }
  }

  override def getSourceDbo(): SourceDao = {
    return sourceDbo
  }

  override def getSearcherDbo(): SearcherDao = {
    return searcherDbo
  }
}
