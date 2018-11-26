package com.stratio.governance.agent.searcher.model

import java.sql.ResultSet

import com.stratio.governance.agent.searcher.model.es.{EntityRowES, FileTableES}

case class FileTable(id: Int,
                     datastore_engine_id: Int,
                     name: String,
                     agent_version: String,
                     server_version: String,
                     operation_command_type: String,
                     created_at: String,
                     updated_at: String,
                     modification_time: Option[Long],
                     access_time: Option[Long],
                     display_name: Option[String],
                     description: Option[String],
                     metadata_path: String,
                     schema_name: String) extends EntityRow

object FileTable {

  val entity = "filetable"

  def entityFromResultSet(fileTable: FileTable, resultSet: ResultSet): scala.Seq[EntityRowES] = {

    val fileTableList: List[(FileTable, KeyValuePair)] = getResult(resultSet, fileTable)

    val fileTableES: Seq[FileTableES] = Seq(FileTableES.
      fromFileTableList(fileTableList))
    fileTableES

  }

  @scala.annotation.tailrec
  def getResult(resultSet: ResultSet, fileTable: FileTable, list: List[(FileTable, KeyValuePair)] = Nil)
  : List[(FileTable, KeyValuePair)] = {
    if (resultSet.next()) {
      val keyValuePair = KeyValuePair.apply(Some(resultSet.getInt(1)),
        resultSet.getInt(2),
        resultSet.getInt(3),
        resultSet.getInt(4),
        resultSet.getString(5),
        resultSet.getString(6),
        resultSet.getString(7),
        resultSet.getString(8),
        resultSet.getString(9),
        resultSet.getString(10),
        resultSet.getString(11),
        resultSet.getString(12),
        Some(resultSet.getLong(13)),
        Some(resultSet.getLong(14)))

      getResult(resultSet, fileTable, (fileTable, keyValuePair) :: list)
    }
    else {
      (fileTable, null) :: list
    }
  }

}