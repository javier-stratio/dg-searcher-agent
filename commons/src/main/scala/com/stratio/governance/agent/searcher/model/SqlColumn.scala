package com.stratio.governance.agent.searcher.model

import java.sql.ResultSet

import com.stratio.governance.agent.searcher.model.es.{EntityRowES, SqlColumnES}

case class SqlColumn(id: Int,
                     sql_table_id: Int,
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
                     operation_command_type: String,
                     nullable: Boolean,
                     metadata: String) extends EntityRow


object SqlColumn {

  val entity = "sqlcolumn"

  def entityFromResultSet(sqlColumn: SqlColumn, resultSet: ResultSet): scala.Seq[EntityRowES] = {

    val sqlColumnList: List[(SqlColumn, KeyValuePair)] = getResult(resultSet, sqlColumn)

    val sqlColumnES: Seq[SqlColumnES] = Seq(SqlColumnES.
      fromSqlColumnList(sqlColumnList))
    sqlColumnES

  }

  @scala.annotation.tailrec
  def getResult(resultSet: ResultSet, sqlColumn: SqlColumn, list: List[(SqlColumn, KeyValuePair)] = Nil)
  : List[(SqlColumn, KeyValuePair)] = {
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

      getResult(resultSet, sqlColumn, (sqlColumn, keyValuePair) :: list)
    }
    else {
      (sqlColumn, null) :: list
    }
  }

}