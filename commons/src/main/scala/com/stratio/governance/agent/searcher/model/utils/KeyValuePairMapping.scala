package com.stratio.governance.agent.searcher.model.utils

import java.sql.ResultSet

import com.stratio.governance.agent.searcher.model.{DatabaseSchema, KeyValuePair}
import com.stratio.governance.agent.searcher.model.es.{DatabaseSchemaES, EntityRowES}

import scala.util.Try

object KeyValuePairMapping {

  val parentTable: Map[Int, String] = Map[Int, String](
    1 -> "database_schema",
    2 -> "sql_table",
    3 -> "sql_column",
    4 -> "file_table",
    5 -> "file_column"
  )

  val parentType: Map[String, Int] = Map[String, Int](
    "database"   -> 1,
    "sqltable"   -> 2,
    "sqlcolumn"  -> 3,
    "filetable"  -> 4,
    "filecolumn" -> 5
  )

  //TODO complete
  def entityFromResultSet(parenType: Int, resultSet: ResultSet): Seq[EntityRowES] = {
    parenType match {
      case 1 =>
        //resultSet has as many records as key value pairs has got the database
        val databaseSchemaList: List[(DatabaseSchema, KeyValuePair)] = getResult(resultSet)

        val databaseSchemaES: Seq[DatabaseSchemaES] = Seq(DatabaseSchemaES.fromDatabaseSchemaList(databaseSchemaList))
        databaseSchemaES
    }
  }

  @scala.annotation.tailrec
  def getResult(resultSet: ResultSet,
                list: List[(DatabaseSchema, KeyValuePair)] = Nil): List[(DatabaseSchema, KeyValuePair)] = {
    if (resultSet.next()) {
      val databaseSchema = DatabaseSchema.apply(
        resultSet.getInt(1),
        resultSet.getInt(2),
        resultSet.getString(3),
        resultSet.getString(4),
        Try(resultSet.getString(5).replaceAll(" ", "T")).getOrElse(null),
        Try(resultSet.getString(6).replaceAll(" ", "T")).getOrElse(null),
        resultSet.getString(7),
        Some(resultSet.getString(8)),
        Some(resultSet.getString(9)),
        resultSet.getString(10),
        Some(resultSet.getLong(11)),
        Some(resultSet.getLong(12)),
        resultSet.getString(13),
        Some(resultSet.getString(14))
      )
      val keyValuePair = KeyValuePair.apply(
        resultSet.getInt(15),
        resultSet.getInt(16),
        resultSet.getInt(17),
        resultSet.getInt(18),
        resultSet.getString(19),
        resultSet.getString(20),
        resultSet.getString(21),
        resultSet.getString(22),
        resultSet.getString(23),
        resultSet.getString(24),
        resultSet.getString(25),
        resultSet.getString(26),
        Some(resultSet.getLong(27)),
        Some(resultSet.getLong(28))
      )

      getResult(resultSet, (databaseSchema, keyValuePair) :: list)
    } else {
      list
    }
  }

}
