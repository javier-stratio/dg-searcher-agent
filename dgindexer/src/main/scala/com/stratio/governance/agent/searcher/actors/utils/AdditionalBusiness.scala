package com.stratio.governance.agent.searcher.actors.utils

import com.stratio.governance.agent.searcher.domain.SearchElementDomain
import com.stratio.governance.agent.searcher.domain.SearchElementDomain.{BusinessAssetReaderElement, QualityRuleReaderElement}
import org.json4s.JsonAST._
import org.slf4j.{Logger, LoggerFactory}

class AdditionalBusiness(dataAssetPrefix: String, businessTermPrefix: String, val btType: String, qualityRulePrefix: String, val qrType: String, val qrSubtype: String) {

  private lazy val LOG: Logger = LoggerFactory.getLogger(getClass.getName)

  private val STORE_WITH_FILES: String = "HDFS"
  private val EXTRA_RESOURCE_FOR_FILES: String = "RESOURCE_FILE"
  private val FILE_DEFINITION_KEY: String = "schema"
  private val FILE_DEFINITION_VALUE: String = "na"

  private val subtypeMap = Map("DS" -> "Data store", "PATH" -> "Path", "RESOURCE" -> "Table", EXTRA_RESOURCE_FOR_FILES -> "File", "FIELD" -> "Column")

  // These three first methods, that use total indexation interface, are wrapped. Partial indexation is used directly

  // Additional union/Query to obtain Business Assets and Quality for Total indexation
  def getAdditionalBusinessTotalIndexationSubquery(schema: String, businessAsset: String, businessAssetType: String, businessAssetStatus: String, quality: String): String = {
    getTotalIndexationQueryInfo(new BusinessAssetReaderElement(schema, businessAsset, businessAssetType, businessAssetStatus, "", ""), btType, "") + " UNION " +
      getTotalIndexationQueryInfo(new QualityRuleReaderElement(schema, quality, "", "", "", ""), qrType, qrSubtype)
  }

  // Additional union/Query to obtain Business Asset from previously retrieved Ids for partial indexation
  def getBusinessAssetsPartialIndexationSubqueryInfoById(schema: String, businessAsset: String, businessAssetType: String, businessAssetStatus: String): String = {
    getTotalIndexationQueryInfo(new BusinessAssetReaderElement(schema, businessAsset, businessAssetType, businessAssetStatus, "", ""), btType, "") + " and ba.id IN({{ids}})"
  }

  // Additional union/Query to obtain QualityRules from previously retrieved Ids for partial indexation
  def getQualityRulesPartialIndexationSubqueryInfoById(schema: String, qualityAsset: String): String = {
    getTotalIndexationQueryInfo(new QualityRuleReaderElement(schema, qualityAsset, "", "", "", ""), qrType, qrSubtype) + " where id IN({{ids}})"
  }

  // Additional Total indexation query parameters for W
  private def getTotalIndexationQueryInfo[W](w: W, typ: String, subTpe: String)(implicit reader: SearchElementDomain.Reader[W]): String = {
    reader.getTotalIndexationQueryInfo(w, typ, subTpe)
  }

  // Retrieve the enriched (id_extended and dataStore) information given certain parameters of a Search Document
  def adaptInfo(id: Int, typ: String, subtype: String, metadataPath: String, properties: JValue): (String, String, String, String) = {
    val idExtended: String = typ match {
      case `btType` =>
        businessTermPrefix + id.toString
      case `qrType` =>
        qualityRulePrefix + id.toString
      case _ =>
        dataAssetPrefix + id.toString
    }
    val dataStore: String = (typ, subtype) match {
      case (_,`qrSubtype`) =>
        qrType
      case (`btType`,_) =>
        btType
      case _ =>
        try {
          metadataPath.substring(0, metadataPath.indexOf(":"))
        } catch {
          case e: Throwable => {
            LOG.warn( "Data Store could not be extracted from metadataPath " + metadataPath )
            ""
          }
        }
    }
    val typeFormatted: String = typ.toLowerCase.capitalize
    val subTypeMapped: Option[String] = subtype match {
      case "RESOURCE" =>
        if (typ == STORE_WITH_FILES) {
          val schema: List[String] = for {
            JObject(child) <- properties
            JField(FILE_DEFINITION_KEY, JString(sch)) <- child
          } yield sch
          if ( !schema.isEmpty && (schema(0) == FILE_DEFINITION_VALUE) ) {
            subtypeMap.get( EXTRA_RESOURCE_FOR_FILES )
          } else {
            subtypeMap.get( subtype )
          }
        } else {
          subtypeMap.get( subtype )
        }
      case _ =>
        subtypeMap.get(subtype)
    }
    (idExtended, dataStore, typeFormatted, if (subTypeMapped.isDefined) subTypeMapped.get else subtype)
  }

  // Check if, given a type, the Search Document belong to an additional Business Item.
  def isAdditionalBusinessItem(typ: String): Boolean = {
    typ match {
      case `btType` =>
        true
      case `qrType` =>
        true
      case _ =>
        false
    }
  }

}
