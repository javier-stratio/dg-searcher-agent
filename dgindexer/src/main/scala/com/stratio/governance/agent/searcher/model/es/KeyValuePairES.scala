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

    val kvpList = databaseSchemaList.filter(_._2 != null)

    val gdp = Try(kvpList.filter(_._2.key == GDP).head._2.value.toBoolean).getOrElse(false)
    val quality = Try(kvpList.filter(_._2.key == QUALITY).head._2.value.toFloat).getOrElse(0F)
    val owner = Try(kvpList.filter(_._2.key == OWNER).head._2.value.toString).getOrElse("")

    KeyValuePairES(gdp, quality, owner)

  }

  def fromFileTableList(fileTable: List[(FileTable, KeyValuePair)])
  : KeyValuePairES = {

    val kvpList = fileTable.filter(_._2 != null)

    val gdp = Try(kvpList.filter(_._2.key == GDP).head._2.value.toBoolean).getOrElse(false)
    val quality = Try(kvpList.filter(_._2.key == QUALITY).head._2.value.toFloat).getOrElse(0F)
    val owner = Try(kvpList.filter(_._2.key == OWNER).head._2.value.toString).getOrElse("")

    KeyValuePairES(gdp, quality, owner)
  }

  def fromFileColumnList(fileColumn: List[(FileColumn, KeyValuePair)])
  : KeyValuePairES = {

    val kvpList = fileColumn.filter(_._2 != null)

    val gdp = Try(kvpList.filter(_._2.key == GDP).head._2.value.toBoolean).getOrElse(false)
    val quality = Try(kvpList.filter(_._2.key == QUALITY).head._2.value.toFloat).getOrElse(0F)
    val owner = Try(kvpList.filter(_._2.key == OWNER).head._2.value.toString).getOrElse("")

    KeyValuePairES(gdp, quality, owner)
  }

  def fromSqlTableList(sqlTable: List[(SqlTable, KeyValuePair)])
  : KeyValuePairES = {

    val kvpList = sqlTable.filter(_._2 != null)

    val gdp = Try(kvpList.filter(_._2.key == GDP).head._2.value.toBoolean).getOrElse(false)
    val quality = Try(kvpList.filter(_._2.key == QUALITY).head._2.value.toFloat).getOrElse(0F)
    val owner = Try(kvpList.filter(_._2.key == OWNER).head._2.value.toString).getOrElse("")

    KeyValuePairES(gdp, quality, owner)
  }

  def fromSqlColumnList(sqlColumn: List[(SqlColumn, KeyValuePair)])
  : KeyValuePairES = {

    val kvpList = sqlColumn.filter(_._2 != null)

    val gdp = Try(kvpList.filter(_._2.key == GDP).head._2.value.toBoolean).getOrElse(false)
    val quality = Try(kvpList.filter(_._2.key == QUALITY).head._2.value.toFloat).getOrElse(0F)
    val owner = Try(kvpList.filter(_._2.key == OWNER).head._2.value.toString).getOrElse("")

    KeyValuePairES(gdp, quality, owner)
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
