package com.stratio.governance.agent.searcher.model

import java.sql.ResultSet

import com.stratio.governance.agent.searcher.model.es.{EntityRowES, FileColumnES, FileTableES}

case class FileColumn(id: Int,
                      file_table_id: Int,
                      `type`: String,
                      agent_version: String,
                      server_version: String,
                      created_at: String,
                      updated_at: String,
                      name: String,
                      display_name: Option[String],
                      description: Option[String],
                      is_pk: Boolean,
                      is_fk: Boolean,
                      is_partk: Boolean,
                      metadata_path: String,
                      modification_time: Option[Long],
                      access_time: Option[Long],
                      operation_command_type: String)
    extends EntityRow

object FileColumn {

  val entity = "filecolumn"

  def entityFromResultSet(fileColumn: FileColumn, resultSet: ResultSet): scala.Seq[EntityRowES] = {

    val fileColumnList: List[(FileColumn, KeyValuePair)] = getResult(resultSet, fileColumn)

    val fileColumnES: Seq[FileColumnES] = Seq(FileColumnES.fromFileColumnList(fileColumnList))
    fileColumnES

  }

  @scala.annotation.tailrec
  def getResult(resultSet: ResultSet,
                fileColumn: FileColumn,
                list: List[(FileColumn, KeyValuePair)] = Nil): List[(FileColumn, KeyValuePair)] = {
    if (resultSet.next()) {
      val keyValuePair = KeyValuePair.apply(
        resultSet.getInt(1),
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
        Some(resultSet.getLong(14))
      )

      getResult(resultSet, fileColumn, (fileColumn, keyValuePair) :: list)
    } else {
      (fileColumn, null) :: list
    }
  }

}
