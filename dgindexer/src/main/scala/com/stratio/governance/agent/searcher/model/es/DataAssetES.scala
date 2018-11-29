package com.stratio.governance.agent.searcher.model.es

import java.sql.Timestamp
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
    return DataAssetES.dateStrToTimestamp(modifiedAt)
  }

  def setModifiedAt(modAt: String): Unit = {
    modifiedAt = modAt
  }

  def addBusinessTerm(bt: String) = {
    if (businessTerms.isEmpty)
      businessTerms = Some(List())
    businessTerms = Some(bt :: businessTerms.get)
  }

  def addKeyValue(k: String, v: String) = {
    if (keyValues.isEmpty)
      keyValues = Some(List())
    keyValues = Some((k,v) :: keyValues.get)
  }

  def getJsonObject(): JValue = {
    jsonObject = jsonObject ~ ("id" -> JInt(id))
    if (!name.isEmpty) jsonObject = jsonObject ~ ("name" -> JString(name.get))
    if (!description.isEmpty) jsonObject = jsonObject ~ ("description" -> JString(description.get))
    jsonObject = jsonObject ~ ("metadataPath" -> JString(metadataPath))
    jsonObject = jsonObject ~ ("type" -> JString(tpe))
    jsonObject = jsonObject ~ ("tenant" -> JString(subtype))
    jsonObject = jsonObject ~ ("active" -> JBool(active))
    jsonObject = jsonObject ~ ("discoveredAt" -> JString(discoveredAt))
    jsonObject = jsonObject ~ ("modifiedAt" -> JString(modifiedAt))
    if (!businessTerms.isEmpty) jsonObject = jsonObject ~ ("businessTerms" -> JArray(businessTerms.get.map(a=>JString(a))))
    if (!keyValues.isEmpty) {
      jsonObject = jsonObject ~ ("keys" -> JArray(keyValues.get.map(a=>JString(a._1))))
      keyValues.get.foreach( a => {
        jsonObject = jsonObject ~ ("key." + a._1 -> JString(a._2))
      })
    }
    return jsonObject
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
    return new SimpleDateFormat(DataAssetES.DATETIME_FORMAT).parse(date).getTime()
  }

  def timestampTodateStr(timestamp: Long): String = {
    return new SimpleDateFormat(DATETIME_FORMAT).format(new Timestamp(timestamp))
  }

}
