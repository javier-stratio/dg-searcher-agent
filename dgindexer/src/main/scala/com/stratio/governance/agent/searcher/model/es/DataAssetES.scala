package com.stratio.governance.agent.searcher.model.es

import java.sql.{ResultSet, Timestamp}

import com.stratio.governance.agent.searcher.model.utils.TimestampUtils
import org.json4s.JsonAST._
import org.json4s.JsonDSL._
import org.slf4j.{Logger, LoggerFactory}

case class DataAssetES(id: Int,
                       name: Option[String],
                       alias: Option[String],
                       description: Option[String],
                       metadataPath: String,
                       tpe: String,
                       subtype: String,
                       tenant: String,
                       active: Boolean,
                       discoveredAt: Timestamp,
                       var modifiedAt: Timestamp) extends EntityRowES {

  private lazy val LOG: Logger = LoggerFactory.getLogger(getClass.getName)

  var jsonObject: JObject = JObject(List())

  var businessTerms: Option[List[String]] = None

  var keyValues: Option[List[(String, String)]] = None

  def getModifiedAt: Long = {
    modifiedAt.getTime
  }

  def getDiscoveredAtAsString: String = {
    TimestampUtils.toString(discoveredAt)
  }

  def getModifiedAtAsString: String = {
    TimestampUtils.toString(modifiedAt)
  }

  def getDataStore: String = {
    try {
      metadataPath.substring(0, metadataPath.indexOf(":"))
    } catch {
      case e: Throwable => {
        LOG.warn("Data Store could not be extracted from metadataPath " + metadataPath)
        ""
      }
    }
  }

  def setModifiedAt(modAt: Timestamp): Unit = {
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

  def getJsonObject: JValue = {
    jsonObject = jsonObject ~ ("id" -> JInt(id))
    if (name.isDefined && (name.get != null)) jsonObject = jsonObject ~ ("name" -> JString(name.get))
    if (alias.isDefined && (alias.get != null)) jsonObject = jsonObject ~ ("alias" -> JString(alias.get))
    if (description.isDefined && (description != null)) jsonObject = jsonObject ~ ("description" -> JString(description.get))
    jsonObject = jsonObject ~ ("metadataPath" -> JString(metadataPath))
    jsonObject = jsonObject ~ ("type" -> JString(tpe))
    jsonObject = jsonObject ~ ("subtype" -> JString(subtype))
    jsonObject = jsonObject ~ ("tenant" -> JString(tenant))
    jsonObject = jsonObject ~ ("active" -> JBool(active))
    jsonObject = jsonObject ~ ("discoveredAt" -> JString(getDiscoveredAtAsString))
    jsonObject = jsonObject ~ ("modifiedAt" -> JString(getModifiedAtAsString))
    jsonObject = jsonObject ~ ("dataStore" -> JString(getDataStore))
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

  @scala.annotation.tailrec
  def getValuesFromResult(resultSet: ResultSet, list: List[DataAssetES] = Nil): List[DataAssetES] = {
    if (resultSet.next()) {
      getValuesFromResult(resultSet, DataAssetES( resultSet.getInt(1),
                                                  Some(resultSet.getString(2)),
                                                  Some(resultSet.getString(3)),
                                                  Some(resultSet.getString(4)),
                                                  resultSet.getString(5),
                                                  resultSet.getString(6),
                                                  resultSet.getString(7),
                                                  resultSet.getString(8),
                                                  resultSet.getBoolean(10),
                                                  resultSet.getTimestamp(11),
                                                  resultSet.getTimestamp(12)) :: list)
    } else {
      list
    }
  }
}
