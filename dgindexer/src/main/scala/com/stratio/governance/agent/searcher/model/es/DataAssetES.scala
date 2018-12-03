package com.stratio.governance.agent.searcher.model.es

import java.sql.{ResultSet, Timestamp}
import java.text.SimpleDateFormat

import com.stratio.governance.commons.agent.domain.dao.DataAssetDao
import org.json4s.JsonAST._
import org.json4s.JsonDSL._

case class DataAssetES(id: Int,
                       name: Option[String],
                       description: Option[String],
                       metadataPath: String,
                       tpe: String,
                       subtype: String,
                       tenant: String,
                       active: Boolean,
                       discoveredAt: String,
                       var modifiedAt: String
                      ) extends EntityRowES {

  var jsonObject: JObject = JObject(List())

  var businessTerms: Option[List[String]] = None

  var keyValues: Option[List[(String, String)]] = None

  def getModifiedAt(): Long = {
    DataAssetES.dateStrToTimestamp(modifiedAt)
  }

  def setModifiedAt(modAt: String): Unit = {
    modifiedAt = modAt
  }

  def addBusinessTerm(bt: String): Unit = {
    if (businessTerms.isEmpty)
      businessTerms = Some(List())
    businessTerms = Some(bt :: businessTerms.get)
  }

  def addKeyValue(k: String, v: String): Unit = {
    if (keyValues.isEmpty)
      keyValues = Some(List())
    keyValues = Some((k,v) :: keyValues.get)
  }

  def getJsonObject(): JValue = {
    jsonObject = jsonObject ~ ("id" -> JInt(id))
    if (name.isDefined) jsonObject = jsonObject ~ ("name" -> JString(name.get))
    if (description.isDefined) jsonObject = jsonObject ~ ("description" -> JString(description.get))
    jsonObject = jsonObject ~ ("metadataPath" -> JString(metadataPath))
    jsonObject = jsonObject ~ ("type" -> JString(tpe))
    jsonObject = jsonObject ~ ("tenant" -> JString(subtype))
    jsonObject = jsonObject ~ ("active" -> JBool(active))
    jsonObject = jsonObject ~ ("discoveredAt" -> JString(discoveredAt))
    jsonObject = jsonObject ~ ("modifiedAt" -> JString(modifiedAt))
    if (businessTerms.isDefined) jsonObject = jsonObject ~ ("businessTerms" -> JArray(businessTerms.get.map(a=>JString(a))))
    if (keyValues.isDefined) {
      jsonObject = jsonObject ~ ("keys" -> JArray(keyValues.get.map(a=>JString(a._1))))
      keyValues.get.foreach( a => {
        jsonObject = jsonObject ~ ("key." + a._1 -> JString(a._2))
      })
    }
    jsonObject
  }

}

object DataAssetES {

  val DATETIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS"

  def fromDataAssetDao(dataAssetDao: DataAssetDao): DataAssetES = {
    DataAssetES(dataAssetDao.id,
      dataAssetDao.name,
      dataAssetDao.description,
      dataAssetDao.metadataPath,
      dataAssetDao.tpe,
      dataAssetDao.subtype,
      dataAssetDao.tenant,
      dataAssetDao.active,
      new SimpleDateFormat(DATETIME_FORMAT).format(dataAssetDao.discoveredAt),
      new SimpleDateFormat(DATETIME_FORMAT).format(dataAssetDao.modifiedAt)
    )
  }

  def dateStrToTimestamp(date: String): Long = {
    new SimpleDateFormat(DataAssetES.DATETIME_FORMAT).parse(date).getTime()
  }

  def timestampTodateStr(timestamp: Long): String = {
    new SimpleDateFormat(DATETIME_FORMAT).format(new Timestamp(timestamp))
  }

  @scala.annotation.tailrec
  def getDataAssetFromResult(resultSet: ResultSet, list: List[DataAssetDao] = Nil): List[DataAssetDao] = {
    if (resultSet.next()) {
      val dataAssetDao = DataAssetDao(id = resultSet.getInt(1),
        name = Some(resultSet.getString(2)),
        description = Some(resultSet.getString(3)),
        metadataPath = resultSet.getString(4),
        tpe = resultSet.getString(5),
        subtype = resultSet.getString(6),
        tenant = resultSet.getString(7),
        properties = resultSet.getString(8),
        active = resultSet.getBoolean(9),
        discoveredAt = resultSet.getTimestamp(10),
        modifiedAt = resultSet.getTimestamp(11))
      getDataAssetFromResult(resultSet, dataAssetDao :: list)
    } else {
      list
    }
  }
}
