package com.stratio.governance.agent.search.testit

import java.sql.Timestamp

import com.stratio.governance.agent.searcher.actors.utils.AdditionalBusiness
import com.stratio.governance.agent.searcher.main.AppConf
import com.stratio.governance.agent.searcher.model.utils.ExponentialBackOff
import com.stratio.governance.agent.search.testit.utils.{PostgresSourceDaoTest, SystemPropertyConfigurator}
import com.stratio.governance.agent.searcher.actors.dao.postgres.PostgresPartialIndexationReadState
import com.stratio.governance.agent.searcher.model.es.ElasticObject
import com.stratio.governance.agent.searcher.model.{BusinessAsset, KeyValuePair, QualityRule}
import com.typesafe.scalalogging.LazyLogging
import org.junit.{FixMethodOrder, Test}
import org.junit.Assert._
import org.junit.runners.MethodSorters
import org.springframework.core.io.support.PathMatchingResourcePatternResolver

@FixMethodOrder( MethodSorters.NAME_ASCENDING )
class PostgresSourceDaoIT extends LazyLogging {

  lazy val exponentialBackOff :ExponentialBackOff = ExponentialBackOff(AppConf.extractorExponentialbackoffPauseMs, AppConf.extractorExponentialbackoffMaxErrorRetry)
  val postgresDao: PostgresSourceDaoTest = new PostgresSourceDaoTest(
    SystemPropertyConfigurator.get(AppConf.sourceConnectionUrl,"SOURCE_CONNECTION_URL"),
    SystemPropertyConfigurator.get(AppConf.sourceConnectionUser,"SOURCE_CONNECTION_USER"),
    SystemPropertyConfigurator.get(AppConf.sourceConnectionPassword,"SOURCE_CONNECTION_PASSWORD"),
    SystemPropertyConfigurator.get(AppConf.sourceDatabase,"SOURCE_DATABASE"),
    AppConf.sourceSchema, 1, 4, exponentialBackOff, new AdditionalBusiness("","bt/", "GLOSSARY","BUSINESS_TERM", "qr/", "QUALITY", "RULES"),true)

  @Test
  def test00beforeAll(): Unit = {
    logger.info("Creating dataBase structure to test ...")

    // Let's create schema
    postgresDao.execute(s"create schema ${AppConf.sourceSchema}")
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
        val scriptSql: String = scala.io.Source.fromInputStream(resource.getInputStream).getLines.mkString("\n")
        postgresDao.execute(scriptSql)
        logger.info(s"${resource.getFilename} executed!!")
      }
    }

    postgresDao.startParent()
    logger.info("DataBase structure created!")

    // Inserting test data
    val testDateFileStream = getClass.getResourceAsStream("/test_data.sql")
    val dataSql: String = scala.io.Source.fromInputStream(testDateFileStream).getLines.mkString("\n")
    postgresDao.execute(dataSql)
    logger.info("Test Data inserted!")
  }

  //"PostgresDao keyValuePairProcess method " should " retrieve all information related" in {
  @Test
  def test01keyValuePairProcess: Unit = {

    val list: List[KeyValuePair] = postgresDao.keyValuePairProcess(List[String]("hdfsFinance://department/marketing/2018>/:region.parquet:R_REGIONKEY:", "hdfsFinance://department/marketing/2017>/:region.parquet:R_REGIONKEY:", "hdfsFinance://department/finance/2018>/:region.parquet:R_COMMENT:"))

    assertEquals(4, list.size)
    assertEquals(3, list.map(_.metadataPath).distinct.size)

  }

  //"PostgresDao businessAssets method " should " retrieve all information related" in {
  @Test
  def test02businessAssetsMethod: Unit = {

    val list: List[BusinessAsset] = postgresDao.businessAssets(List[String]("hdfsFinance://department/marketing/2018>/:region.parquet:R_REGIONKEY:", "hdfsFinance://department/marketing/2017>/:region.parquet:R_REGIONKEY:", "hdfsFinance://department/finance/2018>/:region.parquet:R_COMMENT:"))

    assertEquals(5, list.size)
    assertEquals(3, list.map(_.metadataPath).distinct.size)

  }

  //"PostgresDao businessAssets method " should " retrieve all information related" in {
  @Test
  def test02qualityRulesMethod: Unit = {

    val list: List[QualityRule] = postgresDao.qualityRules(List[String]("hdfsFinance://department/marketing/2018>/:region.parquet:R_REGIONKEY:", "hdfsFinance://department/marketing/2017>/:region.parquet:R_REGIONKEY:", "hdfsFinance://department/finance/2018>/:region.parquet:R_COMMENT:"))

    assertEquals(4, list.size)
    assertEquals(3, list.map(_.metadataPath).distinct.size)

  }

  //"PostgresDao readDataAssetsSince method " should " retrieve all information related" in {
  @Test
  def test03readDataAssetsSinceMethod: Unit = {

    val (list1, next1): (Array[ElasticObject],Int) = postgresDao.readDataAssetsSince(0,6)
    assertEquals(6, list1.size)
    assertEquals(6, next1)

    assertEquals("bt/3", list1(0).id)
    assertEquals("Glossary", list1(0).tpe)
    assertEquals("BUSINESS_TERM", list1(0).subtype)

    assertEquals("qr/3", list1(1).id)
    assertEquals("Quality", list1(1).tpe)
    assertEquals("RULES", list1(1).subtype)


    assertEquals("qr/2", list1(2).id)
    assertEquals("Quality", list1(2).tpe)
    assertEquals("RULES", list1(2).subtype)

    assertEquals("bt/2", list1(3).id)
    assertEquals("Glossary", list1(3).tpe)
    assertEquals("BUSINESS_TERM", list1(3).subtype)



    assertEquals("qr/1", list1(4).id)
    assertEquals("Quality", list1(4).tpe)
    assertEquals("RULES", list1(4).subtype)

    assertEquals("bt/1", list1(5).id)
    assertEquals("Glossary", list1(5).tpe)
    assertEquals("BUSINESS_TERM", list1(5).subtype)


    val (list2,next2): (Array[ElasticObject],Int) = postgresDao.readDataAssetsSince(6,2)
    assertEquals(2, list2.size)
    assertEquals(8, next2)
    assertEquals("201", list2(0).id)
    assertEquals("qr/4", list2(1).id)
    assertEquals("Quality", list2(1).tpe)
    assertEquals("RULES", list2(1).subtype)

    val (list3,next3): (Array[ElasticObject],Int) = postgresDao.readDataAssetsSince(8,2)
    assertEquals(2, list3.size)
    assertEquals(10, next3)
    assertEquals("203", list3(0).id)
    assertEquals("202", list3(1).id)

    val (list4,_): (Array[ElasticObject],Int) = postgresDao.readDataAssetsSince(10,2)
    assertEquals(0, list4.size)

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

    val list: Array[ElasticObject] = postgresDao.readBusinessTermsWhereIdsIn(List[Int](1,2,3))
    assertEquals(3, list.size)
    val ids = list.map(_.id)
    // Order is not defined
    assert(ids.contains("bt/1") && ids.contains("bt/2") && ids.contains("bt/3"))

    val tpe = list.map(_.tpe).distinct
    assertEquals(1, tpe.size)
    assertEquals("Glossary", tpe(0))
    val subtype = list.map(_.subtype).distinct
    assertEquals(1, subtype.size)
    assertEquals("BUSINESS_TERM", subtype(0))

  }

  //"Partial Indexation State cycle " should " be coherent" in {
  @Test
  def test06IndexationStateCycle: Unit = {

    val statusInit = postgresDao.readPartialIndexationState()
    val emptyTs = Timestamp.valueOf("1970-01-01 01:00:0")
    assertEquals(emptyTs, statusInit.readDataAsset)
    assertEquals(emptyTs, statusInit.readBusinessAssets)
    assertEquals(emptyTs, statusInit.readBusinessAssetsDataAsset)
    assertEquals(emptyTs, statusInit.readKey)
    assertEquals(emptyTs, statusInit.readKeyDataAsset)

    val (list1, list2, list3, statusEnd): (List[String], List[Int], List[Int], PostgresPartialIndexationReadState) = postgresDao.readUpdatedDataAssetsIdsSince(statusInit)
    assertEquals(3, list1.size)
    assert(list1.contains("hdfsFinance://department/marketing/2018>/:region.parquet:R_REGIONKEY:") && list1.contains("hdfsFinance://department/marketing/2017>/:region.parquet:R_REGIONKEY:") && list1.contains("hdfsFinance://department/finance/2018>/:region.parquet:R_COMMENT:"))
    assertEquals(3, list2.size)
    assert(list2.contains(1) && list2.contains(2) && list2.contains(3))

    assertEquals(4, list3.size)
    assert(list3.contains(1) && list3.contains(2) && list3.contains(3) && list3.contains(4))

    val refTs = Timestamp.valueOf("2018-12-10 09:27:17.815")
    assertEquals(refTs, statusEnd.readDataAsset)
    assertEquals(refTs, statusEnd.readBusinessAssets)
    assertEquals(refTs, statusEnd.readBusinessAssetsDataAsset)
    assertEquals(refTs, statusEnd.readKey)
    assertEquals(refTs, statusEnd.readKeyDataAsset)

    postgresDao.writePartialIndexationState(statusEnd)

    val statusEndRetrieved = postgresDao.readPartialIndexationState()
    assertEquals(refTs, statusEndRetrieved.readDataAsset)
    assertEquals(refTs, statusEndRetrieved.readBusinessAssets)
    assertEquals(refTs, statusEndRetrieved.readBusinessAssetsDataAsset)
    assertEquals(refTs, statusEndRetrieved.readKey)
    assertEquals(refTs, statusEndRetrieved.readKeyDataAsset)

    val (list1b, list2b, list3b, statusEndRetrievedFixed): (List[String], List[Int], List[Int], PostgresPartialIndexationReadState) = postgresDao.readUpdatedDataAssetsIdsSince(statusInit)
    assertEquals(0,list1b.size)
    assertEquals(0, list2b.size)
    assertEquals(0, list3b.size)
    assertEquals(refTs, statusEndRetrievedFixed.readDataAsset)
    assertEquals(refTs, statusEndRetrievedFixed.readBusinessAssets)
    assertEquals(refTs, statusEndRetrievedFixed.readBusinessAssetsDataAsset)
    assertEquals(refTs, statusEndRetrievedFixed.readKey)
    assertEquals(refTs, statusEndRetrievedFixed.readKeyDataAsset)

  }

  @Test
  def test99afterAll(): Unit = {
    logger.info("Deleting dataBase structure ...")
    postgresDao.execute(s"drop schema ${AppConf.sourceSchema} cascade")
    logger.info("DataBase structure deleted!")
  }

}
