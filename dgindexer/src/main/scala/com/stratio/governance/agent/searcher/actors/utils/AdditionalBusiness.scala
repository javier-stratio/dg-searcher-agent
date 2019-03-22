package com.stratio.governance.agent.searcher.actors.utils

import org.json4s.JsonAST._
import org.slf4j.{Logger, LoggerFactory}

class AdditionalBusiness(dataAssetPrefix: String, businessTermPrefix: String, btType: String, btSubType: String, qualityRulePrefix: String, qrType: String, qrSubtype: String) {

  private lazy val LOG: Logger = LoggerFactory.getLogger(getClass.getName)

  private val STORE_WITH_FILES: String = "HDFS"
  private val EXTRA_RESOURCE_FOR_FILES: String = "RESOURCE_FILE"
  private val FILE_DEFINITION_KEY: String = "schema"
  private val FILE_DEFINITION_VALUE: String = "na"

  private val subtypeMap = Map("DS" -> "Data store", "PATH" -> "Path", "RESOURCE" -> "Table", EXTRA_RESOURCE_FOR_FILES -> "File", "FIELD" -> "Column")

  def getAdditionalBusinessTotalIndexationSubqueryPart1(schema: String, businessAsset: String, businessAssetType: String): String = {
    s"select ba.id as id,ba.name as name,'' as alias,ba.description as description,'' as metadata_path,'$btType' as type,'$btSubType' as subtype,'' as tenant,null as properties,true as active,ba.modified_at as discovered_at,ba.modified_at as modified_at from $schema.$businessAsset as ba, $schema.$businessAssetType as bat where ba.business_assets_type_id = bat.id and bat.name='TERM'"
  }

  def getAdditionalBusinessTotalIndexationSubqueryPart2(schema: String, qualityAsset: String): String = {
    s"select id,name,'' as alias,description,'' as metadata_path,'$qrType' as type,'$qrSubtype' as subtype, tenant,null as properties, active, modified_at as discovered_at, modified_at from $schema.$qualityAsset"
  }

  // Additional union/Query to obtain Business Terms for Total indexation
  def getAdditionalBusinessTotalIndexationSubquery(schema: String, businessAsset: String, businessAssetType: String, qualityAsset: String): String = {
    getAdditionalBusinessTotalIndexationSubqueryPart1(schema, businessAsset, businessAssetType) + " UNION " +
      getAdditionalBusinessTotalIndexationSubqueryPart2(schema, qualityAsset)

  }

  // Additional union/Query to extract Business Terms Ids for partial indexation
  def getAdditionalBusinessPartialIndexationSubquery1(schema: String, businessAssets:  String, businessAssetsType:  String, qualityTable: String, resultNumber: Int): String = {
    s"SELECT '',ba.id,ba.modified_at,$resultNumber FROM $schema.$businessAssets as ba, $schema.$businessAssetsType as bat WHERE ba.business_assets_type_id = bat.id and bat.name='TERM' and ba.modified_at > ? UNION " +
      s"SELECT metadata_path, id, modified_at, ${resultNumber+1} FROM $schema.$qualityTable WHERE modified_at > ?"
  }

  // Additional union/Query to obtain Business Term from previously retrieved Ids for partial indexation
  def getBusinessTermsPartialIndexationSubquery2(schema: String, businessAsset: String, businessAssetType: String): String = {
    getAdditionalBusinessTotalIndexationSubqueryPart1(schema, businessAsset, businessAssetType) + " and ba.id IN({{ids}})"
  }

  // Additional union/Query to obtain QualityRules from previously retrieved Ids for partial indexation
  def getQualityRulesPartialIndexationSubquery2(schema: String, qualityAsset: String): String = {
    getAdditionalBusinessTotalIndexationSubqueryPart2(schema, qualityAsset) + " where id IN({{ids}})"
  }

  // Retrieve the enriched (id_extended and dataStore) information given certain parameters of a Search Document
  def adaptInfo(id: Int, typ: String, subtype: String, metadataPath: String, properties: JValue): (String, String, String, String) = {
    val idExtended: String = subtype match {
      case `btSubType` =>
        businessTermPrefix + id.toString
      case `qrSubtype` =>
        qualityRulePrefix + id.toString
      case _ =>
        dataAssetPrefix + id.toString
    }
    val dataStore: String = subtype match {
      case `btSubType` =>
        btType
      case `qrSubtype` =>
        qrType
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

  // Check if the, given a type, the Search Document belong to an additional Business Item.
  def isAdaptable(typ: String): Boolean = {
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
