package com.stratio.governance.agent.search.testit

import java.sql.Timestamp

import com.stratio.governance.agent.searcher.actors.utils.AdditionalBusiness
import com.stratio.governance.agent.searcher.main.AppConf
import com.stratio.governance.agent.searcher.model.utils.ExponentialBackOff
import com.stratio.governance.agent.search.testit.utils.SystemPropertyConfigurator
import com.stratio.governance.agent.searcher.actors.dao.postgres.{PartialIndexationFields, PostgresPartialIndexationReadState, PostgresSourceDao}
import com.stratio.governance.agent.searcher.model.es.ElasticObject
import com.stratio.governance.agent.searcher.model.{BusinessAsset, BusinessType, KeyValuePair, QualityRule}
import com.typesafe.scalalogging.LazyLogging
import org.junit.{FixMethodOrder, Test}
import org.junit.Assert._
import org.junit.runners.MethodSorters
import org.springframework.core.io.support.PathMatchingResourcePatternResolver

@FixMethodOrder( MethodSorters.NAME_ASCENDING )
class PostgresSourceDaoIT extends LazyLogging {

  lazy val exponentialBackOff :ExponentialBackOff = ExponentialBackOff(AppConf.extractorExponentialbackoffPauseMs, AppConf.extractorExponentialbackoffMaxErrorRetry)
  lazy val schemaTest :String = AppConf.sourceSchema + "_test"

  val postgresDao: PostgresSourceDao = new PostgresSourceDao(
    SystemPropertyConfigurator.get(AppConf.sourceConnectionUrl,"SOURCE_CONNECTION_URL"),
    SystemPropertyConfigurator.get(AppConf.sourceConnectionUser,"SOURCE_CONNECTION_USER"),
    SystemPropertyConfigurator.get(AppConf.sourceConnectionPassword,"SOURCE_CONNECTION_PASSWORD"),
    SystemPropertyConfigurator.get(AppConf.sourceDatabase,"SOURCE_DATABASE"),
    schemaTest, 1, 4, exponentialBackOff, new AdditionalBusiness("","bt/", "GLOSSARY", "qr/", "QUALITY", "RULES", "GENERIC_RULES"),true)

  @Test
  def test00beforeAll(): Unit = {
    logger.info("Creating dataBase structure to test ...")

    // Let's create schema
    postgresDao.execute(s"create schema ${schemaTest}")
    logger.info("schema created")

    // Let's obtain database structure information and insert it
    val scannedPackage = "/BOOT-INF/classes/db/changelog/changes/*.sql"
    val scanner = new PathMatchingResourcePatternResolver
    val resources = scanner.getResources( scannedPackage )

    if ((resources == null) || (resources.length == 0)) {
      throw new Throwable( "Warning: could not find any resources in this scanned package: " + scannedPackage )
    }
    else {
      val orderedResources = resources.sortWith(_.getFilename < _.getFilename)
      for (resource <- orderedResources) {
        logger.info(s"loading ${resource.getFilename} ...")
        val scriptSql: String = scala.io.Source.fromInputStream(resource.getInputStream).getLines.mkString("\n").replace(AppConf.sourceSchema, schemaTest)
        postgresDao.execute(scriptSql)
        logger.info(s"${resource.getFilename} executed!!")
      }
    }

    logger.info("DataBase structure created!")

    // Inserting test data
    val testDateFileStream = getClass.getResourceAsStream("/test_data.sql")
    val dataSql: String = scala.io.Source.fromInputStream(testDateFileStream).getLines.mkString("\n")
    postgresDao.execute(dataSql)
    logger.info("Test Data inserted!")
  }

  //"PostgresDao keyValuePairProcess method " should " retrieve all information related" in {
  @Test
  def test01keyValuePairForDataAsset: Unit = {

    val list: List[KeyValuePair] = postgresDao.keyValuePairForDataAsset(List[String]("hdfsFinance://department/marketing/2018>/:region.parquet:R_REGIONKEY:", "hdfsFinance://department/marketing/2017>/:region.parquet:R_REGIONKEY:", "hdfsFinance://department/finance/2018>/:region.parquet:R_COMMENT:"))

    assertEquals(4, list.size)
    assertEquals(3, list.map(_.identifier).distinct.size)

  }

  //"PostgresDao businessAssets method " should " retrieve all information related" in {
  @Test
  def test02businessAssetsMethod: Unit = {

    val list: List[BusinessAsset] = postgresDao.businessTermsForDataAsset(List[String]("hdfsFinance://department/marketing/2018>/:region.parquet:R_REGIONKEY:", "hdfsFinance://department/marketing/2017>/:region.parquet:R_REGIONKEY:", "hdfsFinance://department/finance/2018>/:region.parquet:R_COMMENT:"))

    assertEquals(5, list.size)
    assertEquals(3, list.map(_.identifier).distinct.size)

  }

  //"PostgresDao businessAssets method " should " retrieve all information related" in {
  @Test
  def test02qualityRulesMethod: Unit = {

    val list: List[QualityRule] = postgresDao.qualityRulesForDataAsset(List[String]("hdfsFinance://department/marketing/2018>/:region.parquet:R_REGIONKEY:", "hdfsFinance://department/marketing/2017>/:region.parquet:R_REGIONKEY:", "hdfsFinance://department/finance/2018>/:region.parquet:R_COMMENT:"))

    assertEquals(5, list.size)
    assertEquals(3, list.map(_.metadataPath).distinct.size)

  }

  //"PostgresDao readDataAssetsSince method " should " retrieve all information related" in {
  @Test
  def test03readDataAssetsSinceMethod: Unit = {

    val (list1, next1): (Array[ElasticObject],Int) = postgresDao.readElementsSince(0,6)
    assertEquals(6, list1.size)
    assertEquals(6, next1)

    assertEquals("bt/3", list1(0).id)
    assertEquals("Glossary", list1(0).tpe)
    assertEquals("Business term", list1(0).subtype)

    assertEquals("qr/3", list1(1).id)
    assertEquals("Quality", list1(1).tpe)
    assertEquals("RULES", list1(1).subtype)


    assertEquals("qr/2", list1(2).id)
    assertEquals("Quality", list1(2).tpe)
    assertEquals("RULES", list1(2).subtype)

    assertEquals("bt/2", list1(3).id)
    assertEquals("Glossary", list1(3).tpe)
    assertEquals("Business term", list1(3).subtype)



    assertEquals("qr/1", list1(4).id)
    assertEquals("Quality", list1(4).tpe)
    assertEquals("RULES", list1(4).subtype)

    assertEquals("bt/1", list1(5).id)
    assertEquals("Glossary", list1(5).tpe)
    assertEquals("Business term", list1(5).subtype)


    val (list2,next2): (Array[ElasticObject],Int) = postgresDao.readElementsSince(6,2)
    assertEquals(2, list2.size)
    assertEquals(8, next2)
    assertEquals("bt/5", list2(0).id)
    assertEquals("Glossary", list2(0).tpe)
    assertEquals("Business rule", list2(0).subtype)
    assertEquals("qr/4", list2(1).id)
    assertEquals("Quality", list2(1).tpe)
    assertEquals("RULES", list2(1).subtype)

    val (list3,next3): (Array[ElasticObject],Int) = postgresDao.readElementsSince(8,2)
    assertEquals(2, list3.size)
    assertEquals(10, next3)
    assertEquals("201", list3(0).id)
    assertEquals("qr/5", list3(1).id)
    assertEquals("Quality", list3(1).tpe)
    assertEquals("GENERIC_RULES", list3(1).subtype)

    val (list4,next4): (Array[ElasticObject],Int) = postgresDao.readElementsSince(10,2)
    assertEquals(2, list4.size)
    assertEquals(12, next4)
    assertEquals("203", list4(0).id)
    assertEquals("202", list4(1).id)

    val (list5,_): (Array[ElasticObject],Int) = postgresDao.readElementsSince(12,2)
    assertEquals(0, list5.size)

  }

  //"PostgresDao readDataAssetsWhereIdsIn method " should " retrieve all information related" in {
  @Test
  def test04readDataAssetsWhereMdpsIn: Unit = {

    val list: Array[ElasticObject] = postgresDao.readDataAssetsWhereMdpsIn(List[String]("hdfsFinance://department/marketing/2018>/:region.parquet:R_REGIONKEY:", "hdfsFinance://department/marketing/2017>/:region.parquet:R_REGIONKEY:", "hdfsFinance://department/finance/2018>/:region.parquet:R_COMMENT:"))
    assertEquals(3, list.size)
    val ids = list.map(_.id)
    // Order is not defined
    assert(ids.contains("201") && ids.contains("202") && ids.contains("203"))

  }

  //"PostgresDao readBusinessTermsWhereIdsIn method " should " retrieve all information related" in {
  @Test
  def test05readBusinessTermsWhereIdsIn: Unit = {

    val list: Array[ElasticObject] = postgresDao.readBusinessAssetsWhereIdsIn(List[Int](1,2,3))
    assertEquals(3, list.size)
    val ids = list.map(_.id)
    // Order is not defined
    assert(ids.contains("bt/1") && ids.contains("bt/2") && ids.contains("bt/3"))

    val tpe = list.map(_.tpe).distinct
    assertEquals(1, tpe.size)
    assertEquals("Glossary", tpe(0))
    val subtype = list.map(_.subtype).distinct
    assertEquals(1, subtype.size)
    assertEquals("Business term", subtype(0))

  }

  //"PostgresDao readQualityRulesWhereIdsIn method " should " retrieve all information related" in {
  @Test
  def test05readQualityWhereIdsIn: Unit = {

    val list: Array[ElasticObject] = postgresDao.readQualityRulesWhereIdsIn(List[Int](1,2,3,4,5,6))
    assertEquals(5, list.size)
    val ids = list.map(_.id)
    // Order is not defined
    assert(ids.contains("qr/1") && ids.contains("qr/2") && ids.contains("qr/3") && ids.contains("qr/4") && ids.contains("qr/5"))

    val tpe = list.map(_.tpe).distinct
    assertEquals(1, tpe.size)
    assertEquals("Quality", tpe(0))
    val subtype = list.map(_.subtype).distinct
    assertEquals(2, subtype.size)
    assertEquals("GENERIC_RULES", subtype(0))
    assertEquals("RULES", subtype(1))

  }

  //"Partial Indexation State cycle " should " be coherent" in {
  @Test
  def test06IndexationStateCycle: Unit = {

    val statusInit = postgresDao.readPartialIndexationState()
    val emptyTs = Timestamp.valueOf("1970-01-01 01:00:0")
    assertEquals(emptyTs, statusInit.getTimeStamp(PartialIndexationFields.DATA_ASSET))
    assertEquals(emptyTs, statusInit.getTimeStamp(PartialIndexationFields.BUSINESS_ASSET))
    assertEquals(emptyTs, statusInit.getTimeStamp(PartialIndexationFields.BUSINESS_ASSET_DATA_ASSET))
    assertEquals(emptyTs, statusInit.getTimeStamp(PartialIndexationFields.KEY))
    assertEquals(emptyTs, statusInit.getTimeStamp(PartialIndexationFields.KEY_DATA_ASSET))
    assertEquals(emptyTs, statusInit.getTimeStamp(PartialIndexationFields.QUALITY_RULE))
    assertEquals(emptyTs, statusInit.getTimeStamp(PartialIndexationFields.KEY_BUSINESS_ASSET))
    assertEquals(emptyTs, statusInit.getTimeStamp(PartialIndexationFields.KEY_QUALITY))
    assertEquals(emptyTs, statusInit.getTimeStamp(PartialIndexationFields.BUSINESS_ASSET_QUALITY))

    val (list1, list2, list3, statusEnd): (List[String], List[Int], List[Int], PostgresPartialIndexationReadState) = postgresDao.readUpdatedElementsIdsSince(statusInit)
    assertEquals(3, list1.size)
    assert(list1.contains("hdfsFinance://department/marketing/2018>/:region.parquet:R_REGIONKEY:") && list1.contains("hdfsFinance://department/marketing/2017>/:region.parquet:R_REGIONKEY:") && list1.contains("hdfsFinance://department/finance/2018>/:region.parquet:R_COMMENT:"))
    assertEquals(4, list2.size)
    assert(list2.contains(1) && list2.contains(2) && list2.contains(3) && list2.contains(5))

    assertEquals(6, list3.size)
    assert(list3.contains(1) && list3.contains(2) && list3.contains(3) && list3.contains(4) && list3.contains(5) && list3.contains(6))

    val refTs = Timestamp.valueOf("2018-12-10 09:27:17.815")
    val refTs2 = Timestamp.valueOf("2019-07-31 06:52:00.238")
    assertEquals(refTs, statusEnd.getTimeStamp(PartialIndexationFields.DATA_ASSET))
    assertEquals(refTs, statusEnd.getTimeStamp(PartialIndexationFields.BUSINESS_ASSET))
    assertEquals(refTs, statusEnd.getTimeStamp(PartialIndexationFields.BUSINESS_ASSET_DATA_ASSET))
    assertEquals(refTs, statusEnd.getTimeStamp(PartialIndexationFields.KEY))
    assertEquals(refTs, statusEnd.getTimeStamp(PartialIndexationFields.KEY_DATA_ASSET))
    assertEquals(refTs, statusEnd.getTimeStamp(PartialIndexationFields.QUALITY_RULE))
    assertEquals(refTs2, statusEnd.getTimeStamp(PartialIndexationFields.KEY_BUSINESS_ASSET))
    assertEquals(refTs2, statusEnd.getTimeStamp(PartialIndexationFields.KEY_QUALITY))
    assertEquals(refTs2, statusEnd.getTimeStamp(PartialIndexationFields.BUSINESS_ASSET_QUALITY))

    postgresDao.writePartialIndexationState(statusEnd)

    val statusEndRetrieved = postgresDao.readPartialIndexationState()
    assertEquals(refTs, statusEndRetrieved.getTimeStamp(PartialIndexationFields.DATA_ASSET))
    assertEquals(refTs, statusEndRetrieved.getTimeStamp(PartialIndexationFields.BUSINESS_ASSET))
    assertEquals(refTs, statusEndRetrieved.getTimeStamp(PartialIndexationFields.BUSINESS_ASSET_DATA_ASSET))
    assertEquals(refTs, statusEndRetrieved.getTimeStamp(PartialIndexationFields.KEY))
    assertEquals(refTs, statusEndRetrieved.getTimeStamp(PartialIndexationFields.KEY_DATA_ASSET))
    assertEquals(refTs, statusEndRetrieved.getTimeStamp(PartialIndexationFields.QUALITY_RULE))
    assertEquals(refTs2, statusEndRetrieved.getTimeStamp(PartialIndexationFields.KEY_BUSINESS_ASSET))
    assertEquals(refTs2, statusEndRetrieved.getTimeStamp(PartialIndexationFields.KEY_QUALITY))
    assertEquals(refTs2, statusEndRetrieved.getTimeStamp(PartialIndexationFields.BUSINESS_ASSET_QUALITY))

    val (list1b, list2b, list3b, statusEndRetrievedFixed): (List[String], List[Int], List[Int], PostgresPartialIndexationReadState) = postgresDao.readUpdatedElementsIdsSince(statusInit)
    assertEquals(0,list1b.size)
    assertEquals(0, list2b.size)
    assertEquals(0, list3b.size)
    assertEquals(refTs, statusEndRetrievedFixed.getTimeStamp(PartialIndexationFields.DATA_ASSET))
    assertEquals(refTs, statusEndRetrievedFixed.getTimeStamp(PartialIndexationFields.BUSINESS_ASSET))
    assertEquals(refTs, statusEndRetrievedFixed.getTimeStamp(PartialIndexationFields.BUSINESS_ASSET_DATA_ASSET))
    assertEquals(refTs, statusEndRetrievedFixed.getTimeStamp(PartialIndexationFields.KEY))
    assertEquals(refTs, statusEndRetrievedFixed.getTimeStamp(PartialIndexationFields.KEY_DATA_ASSET))
    assertEquals(refTs, statusEndRetrievedFixed.getTimeStamp(PartialIndexationFields.QUALITY_RULE))
    assertEquals(refTs2, statusEndRetrievedFixed.getTimeStamp(PartialIndexationFields.KEY_BUSINESS_ASSET))
    assertEquals(refTs2, statusEndRetrievedFixed.getTimeStamp(PartialIndexationFields.KEY_QUALITY))
    assertEquals(refTs2, statusEndRetrievedFixed.getTimeStamp(PartialIndexationFields.BUSINESS_ASSET_QUALITY))

  }

  @Test
  def test07keyValuePairProcessForBusinessAsset: Unit = {

    val list: List[KeyValuePair] = postgresDao.keyValuePairForBusinessAsset(List[Long](1,5))

    assertEquals(2, list.size)
    assertEquals("5", list(0).identifier)
    assertEquals("OWNER", list(0).key)
    assertEquals("{\"name\": \"\", \"value\": \"YourSelf\"}", list(0).value.trim)
    assertEquals("1", list(1).identifier)
    assertEquals("OWNER", list(1).key)
    assertEquals("{\"name\": \"\", \"value\": \"HimSelf\"}", list(1).value.trim)

  }

  @Test
  def test08keyValuePairProcessForBusinessAsset: Unit = {

    val list: List[KeyValuePair] = postgresDao.keyValuePairForQualityRule(List[Long](1))

    assertEquals(1, list.size)
    assertEquals("1", list(0).identifier)
    assertEquals("OWNER", list(0).key)
    assertEquals("{\"name\": \"\", \"value\": \"YourSelf\"}", list(0).value.trim)

  }

  @Test
  def test09businessRulesForQualityRule: Unit = {

    val list: List[BusinessAsset] = postgresDao.businessRulesForQualityRule(List[Long](1))

    assertEquals(1, list.size)
    assertEquals("1", list(0).identifier)
    assertEquals("LegalAge", list(0).name)
    assertEquals(BusinessType.RULE, list(0).tpe)
  }

  @Test
  def test99afterAll(): Unit = {
    logger.info("Deleting dataBase structure ...")
    postgresDao.execute(s"drop schema ${schemaTest} cascade")
    logger.info("DataBase structure deleted!")
  }

}
