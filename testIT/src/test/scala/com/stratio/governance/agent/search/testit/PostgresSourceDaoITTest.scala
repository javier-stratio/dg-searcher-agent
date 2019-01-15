package com.stratio.governance.agent.search.testit

import java.sql.Timestamp

import com.stratio.governance.agent.searcher.actors.utils.AdditionalBusiness
import com.stratio.governance.agent.searcher.main.AppConf
import com.stratio.governance.agent.searcher.model.utils.ExponentialBackOff
import com.stratio.governance.agent.search.testit.utils.PostgresSourceDaoTest
import com.stratio.governance.agent.searcher.actors.dao.postgres.PostgresPartialIndexationReadState
import com.stratio.governance.agent.searcher.model.es.DataAssetES
import com.stratio.governance.agent.searcher.model.{BusinessAsset, KeyValuePair}
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import org.springframework.core.io.support.PathMatchingResourcePatternResolver

class PostgresSourceDaoITTest extends FlatSpec  with BeforeAndAfterAll with  LazyLogging {

  lazy val exponentialBackOff :ExponentialBackOff = ExponentialBackOff(AppConf.extractorExponentialbackoffPauseMs, AppConf.extractorExponentialbackoffMaxErrorRetry)
  val postgresDao: PostgresSourceDaoTest = new PostgresSourceDaoTest(AppConf.sourceConnectionUrl, AppConf.sourceConnectionUser, AppConf.sourceConnectionPassword, AppConf.sourceDatabase, AppConf.sourceSchema, 1, 4, exponentialBackOff, new AdditionalBusiness("","bt/", "GLOSSARY","BUSINESS_TERM"),true)

  override def beforeAll(): Unit = {
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

  override def afterAll(): Unit = {
    logger.info("Deleting dataBase structure ...")
    postgresDao.execute(s"drop schema ${AppConf.sourceSchema} cascade")
    logger.info("DataBase structure deleted!")
  }

  "PostgresDao keyValuePairProcess method " should " retrieve all information related" in {

    val list: List[KeyValuePair] = postgresDao.keyValuePairProcess(Array[Int](201,202,203))

    assertResult(4)(list.size)
    assertResult(3)(list.map(_.id).distinct.size)

  }

  "PostgresDao businessAssets method " should " retrieve all information related" in {

    val list: List[BusinessAsset] = postgresDao.businessAssets(Array[Int](201,202,203))

    assertResult(5)(list.size)
    assertResult(3)(list.map(_.id).distinct.size)

  }

  "PostgresDao readDataAssetsSince method " should " retrieve all information related" in {

    val (list1, next1): (Array[DataAssetES],Int) = postgresDao.readDataAssetsSince(0,2)
    assertResult(2)(list1.size)
    assertResult(2)(next1)
    assertResult("bt/2")(list1(0).id)
    assertResult("Glossary")(list1(0).tpe)
    assertResult("BUSINESS_TERM")(list1(0).subtype)
    assertResult("bt/1")(list1(1).id)
    assertResult("Glossary")(list1(1).tpe)
    assertResult("BUSINESS_TERM")(list1(1).subtype)


    val (list2,next2): (Array[DataAssetES],Int) = postgresDao.readDataAssetsSince(2,2)
    assertResult(2)(list2.size)
    assertResult(4)(next2)
    assertResult("201")(list2(0).id)
    assertResult("bt/3")(list2(1).id)
    assertResult("Glossary")(list2(1).tpe)
    assertResult("BUSINESS_TERM")(list2(1).subtype)

    val (list3,next3): (Array[DataAssetES],Int) = postgresDao.readDataAssetsSince(4,2)
    assertResult(2)(list3.size)
    assertResult(6)(next3)
    assertResult("203")(list3(0).id)
    assertResult("202")(list3(1).id)

    val (list4,_): (Array[DataAssetES],Int) = postgresDao.readDataAssetsSince(6,2)
    assertResult(0)(list4.size)

  }

  "PostgresDao readDataAssetsWhereIdsIn method " should " retrieve all information related" in {

    val list: Array[DataAssetES] = postgresDao.readDataAssetsWhereIdsIn(List[Int](201,202,203))
    assertResult(3)(list.size)
    val ids = list.map(_.id)
    // Order is not defined
    assert(ids.contains("201") && ids.contains("202") && ids.contains("203"))

  }

  "PostgresDao readBusinessTermsWhereIdsIn method " should " retrieve all information related" in {

    val list: Array[DataAssetES] = postgresDao.readBusinessTermsWhereIdsIn(List[Int](1,2,3))
    assertResult(3)(list.size)
    val ids = list.map(_.id)
    // Order is not defined
    assert(ids.contains("bt/1") && ids.contains("bt/2") && ids.contains("bt/3"))

    val tpe = list.map(_.tpe).distinct
    assertResult(1)(tpe.size)
    assertResult("Glossary")(tpe(0))
    val subtype = list.map(_.subtype).distinct
    assertResult(1)(subtype.size)
    assertResult("BUSINESS_TERM")(subtype(0))

  }

  "Partial Indexation State cycle " should " be coherent" in {

    val statusInit = postgresDao.readPartialIndexationState()
    val emptyTs = Timestamp.valueOf("1970-01-01 00:00:0")
    assertResult(emptyTs)(statusInit.readDataAsset)
    assertResult(emptyTs)(statusInit.readBusinessAssets)
    assertResult(emptyTs)(statusInit.readBusinessAssetsDataAsset)
    assertResult(emptyTs)(statusInit.readKey)
    assertResult(emptyTs)(statusInit.readKeyDataAsset)

    val (list1, list2, statusEnd): (List[Int], List[Int], PostgresPartialIndexationReadState) = postgresDao.readUpdatedDataAssetsIdsSince(statusInit)
    assertResult(3)(list1.size)
    assert(list1.contains(201) && list1.contains(202) && list1.contains(203))
    assertResult(3)(list2.size)
    assert(list2.contains(1) && list2.contains(2) && list2.contains(3))
    val refTs = Timestamp.valueOf("2018-12-10 09:27:17.815")
    assertResult(refTs)(statusEnd.readDataAsset)
    assertResult(refTs)(statusEnd.readBusinessAssets)
    assertResult(refTs)(statusEnd.readBusinessAssetsDataAsset)
    assertResult(refTs)(statusEnd.readKey)
    assertResult(refTs)(statusEnd.readKeyDataAsset)

    postgresDao.writePartialIndexationState(statusEnd)

    val statusEndRetrieved = postgresDao.readPartialIndexationState()
    assertResult(refTs)(statusEndRetrieved.readDataAsset)
    assertResult(refTs)(statusEndRetrieved.readBusinessAssets)
    assertResult(refTs)(statusEndRetrieved.readBusinessAssetsDataAsset)
    assertResult(refTs)(statusEndRetrieved.readKey)
    assertResult(refTs)(statusEndRetrieved.readKeyDataAsset)

    val (list1b, list2b, statusEndRetrievedFixed): (List[Int], List[Int], PostgresPartialIndexationReadState) = postgresDao.readUpdatedDataAssetsIdsSince(statusInit)
    assertResult(0)(list1b.size)
    assertResult(0)(list2b.size)
    assertResult(refTs)(statusEndRetrievedFixed.readDataAsset)
    assertResult(refTs)(statusEndRetrievedFixed.readBusinessAssets)
    assertResult(refTs)(statusEndRetrievedFixed.readBusinessAssetsDataAsset)
    assertResult(refTs)(statusEndRetrievedFixed.readKey)
    assertResult(refTs)(statusEndRetrievedFixed.readKeyDataAsset)

  }

}
