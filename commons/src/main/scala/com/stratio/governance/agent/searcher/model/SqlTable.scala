package com.stratio.governance.agent.searcher.model

import java.sql.ResultSet

import com.stratio.governance.agent.searcher.model.es.{EntityRowES, SqlTableES}

case class SqlTable(id: Int,
                    database_schema_id: Int,
                    schema: String,
                    agent_version: String,
                    server_version: String,
                    created_at: String,
                    updated_at: String,
                    name: String,
                    display_name: Option[String],
                    description: Option[String],
                    is_view: Boolean,
                    view_ddl: String,
                    metadata_path: String,
                    modification_time: Option[Long],
                    access_time: Option[Long],
                    operation_command_type: String,
                    tabletype: String,
                    provider: String,
                    partitioncolumnnames: String,
                    stats: String,
                    storage: String)
    extends EntityRow

object SqlTable {

  val entity = "sqltable"

  def entityFromResultSet(sqlTable: SqlTable, resultSet: ResultSet): scala.Seq[EntityRowES] = {

    val sqlTableList: List[(SqlTable, KeyValuePair)] = getResult(resultSet, sqlTable)

    val sqlTableES: Seq[SqlTableES] = Seq(SqlTableES.fromSqlTableList(sqlTableList))
    sqlTableES

  }

  @scala.annotation.tailrec
  def getResult(resultSet: ResultSet,
                sqlTable: SqlTable,
                list: List[(SqlTable, KeyValuePair)] = Nil): List[(SqlTable, KeyValuePair)] = {
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

      getResult(resultSet, sqlTable, (sqlTable, keyValuePair) :: list)
    } else {
      (sqlTable, null) :: list
    }
  }

}
