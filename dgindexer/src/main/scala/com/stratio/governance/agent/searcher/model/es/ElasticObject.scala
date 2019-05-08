package com.stratio.governance.agent.searcher.model.es

import java.sql.{ResultSet, Timestamp}

import com.stratio.governance.agent.searcher.model.utils.TimestampUtils
import org.json4s.JsonAST._
import org.json4s.JsonDSL._
import org.json4s.native.JsonMethods._
import org.slf4j.{Logger, LoggerFactory}

case class ElasticObject(id: String,
                         name: Option[String],
                         alias: Option[String],
                         description: Option[String],
                         metadataPath: String,
                         tpe: String,
                         subtype: String,
                         tenant: String,
                         active: Boolean,
                         discoveredAt: Timestamp,
                         var modifiedAt: Timestamp) {

  private lazy val LOG: Logger = LoggerFactory.getLogger(getClass.getName)

  var jsonObject: JObject = JObject(List())

  var businessTerms: Option[List[String]] = None

  var qualityRules: Option[List[String]] = None

  var keyValues: Option[List[(String, String)]] = None

  var dataStore: String = ""

  def getModifiedAt: Long = {
    modifiedAt.getTime
  }

  def getDiscoveredAtAsString: String = {
    TimestampUtils.toString(discoveredAt)
  }

  def getModifiedAtAsString: String = {
    TimestampUtils.toString(modifiedAt)
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

  def addQualityRule(qr: String): Unit = {
    if (qualityRules.isEmpty)
      qualityRules = Some(List())
    qualityRules = Some(qr :: qualityRules.get)
  }

  def getJsonObject: JValue = {
    jsonObject = jsonObject ~ ("id" -> JString(id))
    jsonObject = jsonObject ~ ("name" -> JString(if (name.isDefined && name.get != null) name.get else ""))
    jsonObject = jsonObject ~ ("alias" -> JString(if (alias.isDefined && alias.get != null) alias.get else ""))
    jsonObject = jsonObject ~ ("description" -> JString(if (description.isDefined && description.get != null) description.get else ""))
    jsonObject = jsonObject ~ ("metadataPath" -> JString(metadataPath))
    jsonObject = jsonObject ~ ("type" -> JString(tpe))
    jsonObject = jsonObject ~ ("subtype" -> JString(subtype))
    jsonObject = jsonObject ~ ("tenant" -> JString(tenant))
    jsonObject = jsonObject ~ ("active" -> JBool(active))
    jsonObject = jsonObject ~ ("discoveredAt" -> JString(getDiscoveredAtAsString))
    jsonObject = jsonObject ~ ("modifiedAt" -> JString(getModifiedAtAsString))
    jsonObject = jsonObject ~ ("dataStore" -> JString(dataStore))
    if (businessTerms.isDefined) jsonObject = jsonObject ~ ("businessTerms" -> JArray(businessTerms.get.map(a=>JString(a))))
    if (qualityRules.isDefined) jsonObject = jsonObject ~ ("qualityRules" -> JArray(qualityRules.get.map(a=>JString(a))))
    if (keyValues.isDefined) {
      jsonObject = jsonObject ~ ("keys" -> JArray(keyValues.get.map(a=>JString(a._1))))
      keyValues.get.foreach( a => {
        jsonObject = jsonObject ~ ("key." + a._1 -> JString(a._2))
      })
    }
    jsonObject
  }

}

object ElasticObject {

  @scala.annotation.tailrec
  def getValuesFromResult(f: (Int, String, String, String, JValue) => (String, String, String, String), resultSet: ResultSet, list: List[ElasticObject] = Nil): List[ElasticObject] = {
    if (resultSet.next()) {
      val id = resultSet.getInt(1)
      val metadataPath = resultSet.getString(5)
      val typ = resultSet.getString(6)
      val subType = resultSet.getString(7)
      val jValue = parseProperties(resultSet.getString(9))
      val calculated_values: (String, String, String, String) = f(id, typ, subType, metadataPath, jValue)
      val daEs = ElasticObject( calculated_values._1,
        Some(resultSet.getString(2)),
        Some(resultSet.getString(3)),
        Some(resultSet.getString(4)),
        metadataPath,
        calculated_values._3,
        calculated_values._4,
        resultSet.getString(8),
        resultSet.getBoolean(10),
        resultSet.getTimestamp(11),
        resultSet.getTimestamp(12))
      daEs.dataStore = calculated_values._2
      getValuesFromResult(f, resultSet, daEs :: list)
    } else {
      list
    }
  }

  private def parseProperties(properties: String): JValue = {
    properties match {
      case null =>
        parse("{}")
      case "" =>
        parse("{}")
      case _ =>
        parse(properties)
    }
  }

}
