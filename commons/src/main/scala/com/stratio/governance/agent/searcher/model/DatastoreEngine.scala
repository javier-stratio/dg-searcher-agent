package com.stratio.governance.agent.searcher.model

import java.sql.ResultSet

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
                           access_time: Option[Long]
                           ) extends EntityRow

object DatastoreEngine{

  @scala.annotation.tailrec
  def getResult(resultSet: ResultSet, list: List[DatastoreEngine] = Nil): List[DatastoreEngine] = {
    if (resultSet.next()) {
      val value = DatastoreEngine.apply( resultSet.getInt(1),
        resultSet.getString(2),
        Some(resultSet.getString(3)),
        resultSet.getString(4),
        resultSet.getString(5),
        resultSet.getString(6),
        resultSet.getString(7),
        resultSet.getString(8),
        resultSet.getString(9),
        resultSet.getString(10),
        Some(resultSet.getLong(11)),
        Some(resultSet.getLong(12))
      )
      getResult(resultSet, value :: list)
    }
    else {
      list
    }
  }


  val entity = "datastore"
}
