package com.stratio.governance.agent.searcher.model.es

import com.stratio.governance.agent.searcher.model._

import scala.util.Try

case class KeyValuePairES(gdp: Boolean,
                          quality: Float,
                          owner: String)

object KeyValuePairES {

  val GDP = "gdp"
  val QUALITY = "quality"
  val OWNER = "owner"

  //TODO these functions are identical. Merge into one using EntityRow and test them

  def fromDatabaseSchemaList(databaseSchemaList: List[(DatabaseSchema, KeyValuePair)])
  : KeyValuePairES = {

    databaseSchemaList.head._2 match {
      case null => null
      case _ =>     KeyValuePairES(
        Try(databaseSchemaList.filter(_._2.key == GDP).head._2.value.toBoolean).getOrElse(false),
        Try(databaseSchemaList.filter(_._2.key == QUALITY).head._2.value.toFloat).getOrElse(0),
        Try(databaseSchemaList.filter(_._2.key == OWNER).head._2.value.toString).getOrElse("")
      )

    }
  }

  def fromFileTableList(fileTable: List[(FileTable, KeyValuePair)])
  : KeyValuePairES = {

    fileTable.head._2 match {
      case null => null
      case _ =>     KeyValuePairES(
        Try(fileTable.filter(_._2.key == GDP).head._2.value.toBoolean).getOrElse(false),
        Try(fileTable.filter(_._2.key == QUALITY).head._2.value.toFloat).getOrElse(0),
        Try(fileTable.filter(_._2.key == OWNER).head._2.value.toString).getOrElse("")
      )

    }
  }

  def fromFileColumnList(fileColumn: List[(FileColumn, KeyValuePair)])
  : KeyValuePairES = {

    fileColumn.head._2 match {
      case null => null
      case _ =>     KeyValuePairES(
        Try(fileColumn.filter(_._2.key == GDP).head._2.value.toBoolean).getOrElse(false),
        Try(fileColumn.filter(_._2.key == QUALITY).head._2.value.toFloat).getOrElse(0),
        Try(fileColumn.filter(_._2.key == OWNER).head._2.value.toString).getOrElse("")
      )

    }
  }

  def fromSqlTableList(sqlTable: List[(SqlTable, KeyValuePair)])
  : KeyValuePairES = {

    sqlTable.head._2 match {
      case null => null
      case _ => KeyValuePairES(
        Try(sqlTable.filter(_._2.key == GDP).head._2.value.toBoolean).getOrElse(false),
        Try(sqlTable.filter(_._2.key == QUALITY).head._2.value.toFloat).getOrElse(0),
        Try(sqlTable.filter(_._2.key == OWNER).head._2.value.toString).getOrElse("")
      )

    }
  }

  def fromSqlColumnList(sqlColumn: List[(SqlColumn, KeyValuePair)])
    : KeyValuePairES = {

    sqlColumn.head._2 match {
      case null => null
      case _ => KeyValuePairES(
        Try(sqlColumn.filter(_._2.key == GDP).head._2.value.toBoolean).getOrElse(false),
        Try(sqlColumn.filter(_._2.key == QUALITY).head._2.value.toFloat).getOrElse(0),
        Try(sqlColumn.filter(_._2.key == OWNER).head._2.value.toString).getOrElse("")
      )

    }
  }


  ////

  def fromKeyValuePair(keyValuePair: KeyValuePair): KeyValuePairES = {
    KeyValuePairES(true, "1.0".toFloat, "owner")
  }


  def fromDatastoreEngine(datastoreEngine: DatastoreEngine): KeyValuePairES = {
    //TODO these values must be recovered from database
    KeyValuePairES(true, "1.0".toFloat, "owner")
  }
}
