package com.stratio.governance.agent.searcher.actors.utils

import com.stratio.governance.commons.MetadataPath
import org.json4s.native.JsonMethods._
import org.scalatest.FlatSpec

class AdditionalBusinessUnitTest extends FlatSpec {

  val additionalBusiness: AdditionalBusiness = new AdditionalBusiness("","bt/", "GLOSSARY", "BUSINESS_TERM", "qr/", "QUALITY", "RULES")

  "method getBTTotalIndexationsubquery" should "be processed properly" in {

    val result: String = additionalBusiness.getAdditionalBusinessTotalIndexationSubquery("dg_metadata", "business_assets", "business_assets_type", "quality")
    assertResult("select ba.id as id,ba.name as name,'' as alias,ba.description as description,'' as metadata_path,'GLOSSARY' as type,'BUSINESS_TERM' as subtype,ba.tenant,null as properties,true as active,ba.modified_at as discovered_at,ba.modified_at as modified_at " +
      "from dg_metadata.business_assets as ba, dg_metadata.business_assets_type as bat where ba.business_assets_type_id = bat.id and bat.name='TERM' " +
      "UNION " +
      "select id,name,'' as alias,description,'' as metadata_path,'QUALITY' as type,'RULES' as subtype, tenant,null as properties, active, modified_at as discovered_at, modified_at from dg_metadata.quality")(result)

  }

  "method getBTPartialIndexationSubquery1" should "be processed properly" in {

    val result: String = additionalBusiness.getAdditionalBusinessPartialIndexationSubquery1("dg_metadata", "business_assets", "business_assets_type", "quality", 7)
    assertResult("SELECT '',ba.id,ba.modified_at,7 " +
      "FROM dg_metadata.business_assets as ba, dg_metadata.business_assets_type as bat " +
      "WHERE ba.business_assets_type_id = bat.id and bat.name='TERM' and ba.modified_at > ? UNION " +
      "SELECT metadata_path, id, modified_at, 8 FROM dg_metadata.quality WHERE modified_at > ?")(result)

  }

  "method getBTPartialIndexationSubquery2" should "be processed properly" in {

    val result: String = additionalBusiness.getBusinessTermsPartialIndexationSubquery2("dg_metadata", "business_assets", "business_assets_type")
    assertResult("select ba.id as id,ba.name as name,'' as alias,ba.description as description,'' as metadata_path,'GLOSSARY' as type,'BUSINESS_TERM' as subtype,ba.tenant,null as properties,true as active,ba.modified_at as discovered_at,ba.modified_at as modified_at from dg_metadata.business_assets as ba, dg_metadata.business_assets_type as bat where ba.business_assets_type_id = bat.id and bat.name='TERM' and ba.id IN({{ids}})")(result)

  }

  "method getQualityRulesPartialIndexationSubquery2" should "be processed properly" in {

    val result: String = additionalBusiness.getQualityRulesPartialIndexationSubquery2("dg_metadata", "quality")
    assertResult("select id,name,'' as alias,description,'' as metadata_path,'QUALITY' as type,'RULES' as subtype, tenant,null as properties, active, modified_at as discovered_at, modified_at from dg_metadata.quality where id IN({{ids}})")(result)

  }

  "method adaptInfo Resources" should "be processed properly" in {

    val result: (String, String, String, String) = additionalBusiness.adaptInfo(1, "HDFS", "RESOURCE", MetadataPath.factory("ds", Some("/path"), Some("resource"), None).toString(), parse("{\"hdfsFile\":{\"schema\":\"parquet\",\"type\":\"whatever\"}}"))
    assertResult("1")(result._1)
    assertResult("ds")(result._2)
    assertResult("Hdfs")(result._3)
    assertResult("Table")(result._4)

    val result2: (String, String, String, String) = additionalBusiness.adaptInfo(1, "HDFS", "RESOURCE", MetadataPath.factory("ds", Some("/path"), Some("resource"), None).toString(), parse("{\"hdfsFile\":{\"schema\":\"na\",\"type\":\"whatever\"}}"))
    assertResult("1")(result2._1)
    assertResult("ds")(result2._2)
    assertResult("Hdfs")(result2._3)
    assertResult("File")(result2._4)

    val result3: (String, String, String, String) = additionalBusiness.adaptInfo(1, "SQL", "RESOURCE", MetadataPath.factory("ds", Some("/path"), Some("resource"), None).toString(), parse("{\"hdfsFile\":{\"schema\":\"na\",\"type\":\"whatever\"}}"))
    assertResult("1")(result3._1)
    assertResult("ds")(result3._2)
    assertResult("Sql")(result3._3)
    assertResult("Table")(result3._4)

  }

  "method adaptInfo No Resources" should "be processed properly" in {

    val result: (String, String, String, String) = additionalBusiness.adaptInfo(1, "HDFS", "DS", MetadataPath.factory("ds", None, None, None).toString(), parse("{\"dataStore\":{\"prop\":\"any\",\"type\":\"whatever\"}}"))
    assertResult("1")(result._1)
    assertResult("ds")(result._2)
    assertResult("Hdfs")(result._3)
    assertResult("Data store")(result._4)

    val result2: (String, String, String, String) = additionalBusiness.adaptInfo(1, "HDFS", "PATH", MetadataPath.factory("ds", Some("/path"), None, None).toString(), parse("{\"hdfsDir\":{\"prop\":\"any\",\"type\":\"whatever\"}}"))
    assertResult("1")(result2._1)
    assertResult("ds")(result2._2)
    assertResult("Hdfs")(result2._3)
    assertResult("Path")(result2._4)

    val result3: (String, String, String, String) = additionalBusiness.adaptInfo(1, "HDFS", "FIELD", MetadataPath.factory("ds", Some("/path"), Some("resource"), Some("field")).toString(), parse("{\"hdfsColumn\":{\"prop\":\"any\",\"type\":\"whatever\"}}"))
    assertResult("1")(result3._1)
    assertResult("ds")(result3._2)
    assertResult("Hdfs")(result3._3)
    assertResult("Column")(result3._4)

  }


  "method isAdaptable" should "be processed properly" in {

    val result: Boolean = additionalBusiness.isAdaptable("GLOSSARY")
    assert(result)

    val result2: Boolean = additionalBusiness.isAdaptable("whatever")
    assert(!result2)

  }

}
