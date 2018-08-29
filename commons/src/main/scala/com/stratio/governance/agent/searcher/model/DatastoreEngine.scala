package com.stratio.governance.agent.searcher.model

import java.sql.ResultSet

import scalikejdbc.DB
import scalikejdbc._
import scalikejdbc.streams._

import scala.concurrent.ExecutionContext

case class DatastoreEngine(id: Int,
                           `type`: String,
                           credentials: Option[String],
                           agent_version: String,
                           server_version: String,
                           created_at: String,
                           updated_at: String,
                           metadata_path: String,
                           name: String,
                           operation_command_type: String,
                           modification_time: Option[Long],
                           access_time: Option[Long])
    extends EntityRow

object DatastoreEngine {

  @scala.annotation.tailrec
  def getResult(resultSet: ResultSet, list: List[DatastoreEngine] = Nil): List[DatastoreEngine] = {
    if (resultSet.next()) {
      val value = DatastoreEngine.apply(
        resultSet.getInt(1),
        resultSet.getString(2),
        Some(resultSet.getString(3)),
        resultSet.getString(4),
        resultSet.getString(5),
        resultSet.getString(6).replaceAll(" ", "T"),
        resultSet.getString(7).replaceAll(" ", "T"),
        resultSet.getString(8),
        resultSet.getString(9),
        resultSet.getString(10),
        Some(resultSet.getLong(11)),
        Some(resultSet.getLong(12))
      )
      getResult(resultSet, value :: list)
    } else {
      list
    }
  }

  def getOneResult(resultSet: WrappedResultSet): DatastoreEngine = {
    DatastoreEngine.apply(
      resultSet.int(1),
      resultSet.string(2),
      resultSet.stringOpt(3),
      resultSet.string(4),
      resultSet.string(5),
      resultSet.string(6).replaceAll(" ", "T"),
      resultSet.string(7).replaceAll(" ", "T"),
      resultSet.string(8),
      resultSet.string(9),
      resultSet.string(10),
      resultSet.longOpt(11),
      resultSet.longOpt(12)
    )
  }

  def publisher(implicit executionContext: ExecutionContext): DatabasePublisher[DatastoreEngine] = DB.readOnlyStream {
    sql"SELECT * FROM dg_metadata.datastore_engine"
      .map(r => DatastoreEngine.getOneResult(r))
      .iterator
      .withDBSessionForceAdjuster(session => {
        session.connection.setAutoCommit(false)
        session.fetchSize(10)
      })
  }

  val entity = "datastore"
}
