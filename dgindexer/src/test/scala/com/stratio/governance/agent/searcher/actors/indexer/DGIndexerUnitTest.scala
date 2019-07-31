package com.stratio.governance.agent.searcher.actors.indexer

import java.sql.{PreparedStatement, ResultSet, Timestamp}
import java.util.concurrent.Semaphore

import akka.actor.{Actor, ActorRef, Cancellable}
import com.stratio.governance.agent.searcher.actors.SearcherActorSystem
import com.stratio.governance.agent.searcher.actors.dao.postgres.PostgresPartialIndexationReadState
import com.stratio.governance.agent.searcher.actors.dao.searcher.DGSearcherDaoException
import com.stratio.governance.agent.searcher.actors.extractor.DGExtractorParams
import com.stratio.governance.agent.searcher.actors.extractor.dao.{SourceDao => ExtractorSourceDao}
import com.stratio.governance.agent.searcher.actors.indexer.dao.{SearcherDao, SourceDao => IndexerSourceDao}
import com.stratio.governance.agent.searcher.actors.utils.AdditionalBusiness
import com.stratio.governance.agent.searcher.model.es.ElasticObject
import com.stratio.governance.agent.searcher.model.utils.ExponentialBackOff
import com.stratio.governance.agent.searcher.model.{BusinessAsset, EntityRow, KeyValuePair, QualityRule}
import org.scalatest.FlatSpec
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class ExtractorTestParams(s: Semaphore, sourceDao: CustomTestSourceDao, chunk: Array[ElasticObject]) extends DGExtractorParams(sourceDao, 10,10, ExponentialBackOff(10, 10),10,"test") {

  def getSemaphore: Semaphore = {
    s
  }

  def getChunk: Array[ElasticObject] = {
    chunk
  }

}
class CustomTestSourceDao(noAdds: Boolean, value_type: Option[String]) extends ExtractorSourceDao with IndexerSourceDao {
  override def keyValuePairProcess(mdps: List[String]): List[KeyValuePair] = {
    if (!noAdds) {
      val value_quality_json: String = value_type match {
        case Some("STRING") => "{\"name\":\"\",\"value\":\"High\"}"
        case Some("BOOLEAN") => "{\"name\":\"\",\"value\":true}"
        case Some("NUMBER") => "{\"name\":\"\",\"value\":1234}"
        case Some("NULL") => "{\"name\":\"\",\"value\":null}"
        case Some("EMPTY") => "{\"name\":\"\",\"value\":\"\"}"
        case _ => ""
      }
      val rows: List[List[KeyValuePair]] = mdps.map(md => List(KeyValuePair(md, "OWNER", "{\"name\":\"\",\"value\":\"finantial\"}", "2018-11-29T10:27:00.000"), KeyValuePair(md, "QUALITY", value_quality_json, "2018-09-28T20:45:00.000")))
      rows.fold[List[KeyValuePair]](List())((a: List[KeyValuePair], b: List[KeyValuePair]) => {
        a ++ b
      }).filter(a => a.getMatadataPath != "EmptyDatastore:")
    } else {
      List[KeyValuePair]()
    }
  }

  override def businessAssets(mdps: List[String]): List[BusinessAsset] = {
    if (!noAdds) {
      val rows: List[List[BusinessAsset]] = mdps.map(md => List(BusinessAsset(md, "RGDP", "RGDP law","APR","TERM","2018-09-28T20:45:00.000"), BusinessAsset(md, "FINANTIAL", "FINANTIAL law","APR","TERM" , "2018-09-28T20:45:00.000")))
      rows.fold[List[BusinessAsset]](List())((a: List[BusinessAsset], b: List[BusinessAsset]) => {
        a ++ b
      }).filter(a => a.getMatadataPath != "EmptyDatastore:")
    } else {
      List[BusinessAsset]()
    }
  }
  override def qualityRules(mdps: List[String]): List[QualityRule] = {
    if (!noAdds) {
      val rows: List[List[QualityRule]] = mdps.map(md => List(QualityRule(md, "qualityRule1", "2018-09-28T20:45:00.000"), QualityRule(md, "qualityRule2", "2018-09-28T20:45:00.000")))
      rows.fold[List[QualityRule]](List())((a: List[QualityRule], b: List[QualityRule]) => {
        a ++ b
      }).filter(a => a.getMatadataPath != "EmptyDatastore:")
    } else {
      List[QualityRule]()
    }
  }


  override def close(): Unit = ???

  override def readDataAssetsSince(offset: Int, limit: Int): (Array[ElasticObject], Int) = ???

  override def readDataAssetsWhereMdpsIn(ids: List[String]): Array[ElasticObject] = ???

  override def readUpdatedDataAssetsIdsSince(state: PostgresPartialIndexationReadState): (List[String], List[Int], List[Int], PostgresPartialIndexationReadState) = ???

  override def readPartialIndexationState(): PostgresPartialIndexationReadState = ???

  override def writePartialIndexationState(state: PostgresPartialIndexationReadState): Unit = ???

  override def prepareStatement(queryName: String): PreparedStatement = ???

  override def executeQuery(sql: String): ResultSet = ???

  override def execute(sql: String): Unit = ???

  override def executePreparedStatement(sql: PreparedStatement): Unit = ???

  override def executeQueryPreparedStatement(sql: PreparedStatement): ResultSet = ???

  override def readBusinessTermsWhereIdsIn(ids: List[Int]): Array[ElasticObject] = ???

  override def readQualityRulesWhereIdsIn(ids: List[Int]): Array[ElasticObject] = ???
}

class PartialIndexerTestParams(s: Semaphore, noAdds: Boolean, value_type: Option[String]) extends IndexerParams {

  var result: String = ""

  def getSemaphore: Semaphore = {
    s
  }

  override def getSourceDao: CustomTestSourceDao = new CustomTestSourceDao(noAdds, value_type)

  override def getSearcherDao: SearcherDao = new SearcherDao {

    override def indexPartial(model: String, doc: String): Option[DGSearcherDaoException] = {
      result = doc
      s.release()
      None
    }

    override def indexTotal(model: String, doc: String, token: String): Option[DGSearcherDaoException] = {
      result = doc
      s.release()
      None
    }
  }

  def getResult: String = {
    result
  }

  override def getPartition: Int = {
    2
  }

  override def getAdditionalBusiness: AdditionalBusiness =   new AdditionalBusiness("da/", "bt/", "GLOSSARY", "BUSSINESS_TERMS", "qr/", "QUALITY", "RULES")
}

class SASTExtractor(indexer: ActorRef, params: ExtractorTestParams) extends Actor {

  private final val NOTIFICATION: String = "notification"

  private lazy val LOG: Logger = LoggerFactory.getLogger(getClass.getName)

  val notification_cancellable: Cancellable = context.system.scheduler.scheduleOnce(1 millis, self, NOTIFICATION)

  override def receive: PartialFunction[Any, Unit] = {
    case NOTIFICATION =>
      println("Notification received")
      indexer ! DGIndexer.IndexerEvent(params.getChunk, None)
    case _ =>
      LOG.info("Extractor default handle. Nothing to do.")
  }

}

class DGIndexerUnitTest extends FlatSpec {

  "Extractor Completed Events Simulation with text typed attribute" should "be processed in Indexer Mock" in {

    val milis: Long = 1543424486000l
    val reference: String = "[{\"id\":\"da/2\",\"name\":\"MyDataStore\",\"alias\":\"\",\"description\":\"My DataStore\",\"metadataPath\":\"MyDataStore:\",\"type\":\"SQL\",\"subtype\":\"DS\",\"_tenant\":\"stratio\",\"active\":true,\"discoveredAt\":\"2018-11-28T17:01:26.000\",\"modifiedAt\":\"2018-11-29T10:27:00.000\",\"dataStore\":\"MyDataStore\",\"businessTerms\":[\"FINANTIAL\",\"RGDP\"],\"qualityRules\":[\"qualityRule2\",\"qualityRule1\"],\"keys\":[\"QUALITY\",\"OWNER\"],\"keysValues\":[\"QUALITY:=High\",\"OWNER:=finantial\"]},{\"id\":\"da/1\",\"name\":\"EmptyStore\",\"alias\":\"TheOnlyOne\",\"description\":\"Empty DataStore\",\"metadataPath\":\"EmptyDatastore:\",\"type\":\"HDFS\",\"subtype\":\"DS\",\"_tenant\":\"stratio\",\"active\":true,\"discoveredAt\":\"2018-11-28T17:01:26.000\",\"modifiedAt\":\"2018-11-28T17:01:26.000\",\"dataStore\":\"EmptyDatastore\"},{\"id\":\"da/3\",\"name\":\"FinantialDB\",\"alias\":\"\",\"description\":\"Finantial DataBase\",\"metadataPath\":\"MyDataStore://>FinantialDB/:\",\"type\":\"SQL\",\"subtype\":\"PATH\",\"_tenant\":\"stratio\",\"active\":true,\"discoveredAt\":\"2018-11-28T17:01:26.000\",\"modifiedAt\":\"2018-11-29T10:27:00.000\",\"dataStore\":\"MyDataStore\",\"businessTerms\":[\"FINANTIAL\",\"RGDP\"],\"qualityRules\":[\"qualityRule2\",\"qualityRule1\"],\"keys\":[\"QUALITY\",\"OWNER\"],\"keysValues\":[\"QUALITY:=High\",\"OWNER:=finantial\"]},{\"id\":\"da/4\",\"name\":\"toys-department\",\"alias\":\"\",\"description\":\"Toys Department\",\"metadataPath\":\"MyDataStore://>FinantialDB/:toys-department\",\"type\":\"SQL\",\"subtype\":\"RESOURCE\",\"_tenant\":\"stratio\",\"active\":true,\"discoveredAt\":\"2018-11-28T17:01:26.000\",\"modifiedAt\":\"2018-11-29T10:27:00.000\",\"dataStore\":\"MyDataStore\",\"businessTerms\":[\"FINANTIAL\",\"RGDP\"],\"qualityRules\":[\"qualityRule2\",\"qualityRule1\"],\"keys\":[\"QUALITY\",\"OWNER\"],\"keysValues\":[\"QUALITY:=High\",\"OWNER:=finantial\"]},{\"id\":\"da/5\",\"name\":\"purchaserName\",\"alias\":\"\",\"description\":\"Purchaser Name\",\"metadataPath\":\"MyDataStore://>FinantialDB/:toys-department:purchaserName:\",\"type\":\"SQL\",\"subtype\":\"FIELD\",\"_tenant\":\"stratio\",\"active\":true,\"discoveredAt\":\"2018-11-28T17:01:26.000\",\"modifiedAt\":\"2018-11-29T10:27:00.000\",\"dataStore\":\"MyDataStore\",\"businessTerms\":[\"FINANTIAL\",\"RGDP\"],\"qualityRules\":[\"qualityRule2\",\"qualityRule1\"],\"keys\":[\"QUALITY\",\"OWNER\"],\"keysValues\":[\"QUALITY:=High\",\"OWNER:=finantial\"]},{\"id\":\"da/6\",\"name\":\"purchases\",\"alias\":\"\",\"description\":\"Purchases\",\"metadataPath\":\"MyDataStore://>FinantialDB/:toys-department:purchases:\",\"type\":\"SQL\",\"subtype\":\"FIELD\",\"_tenant\":\"stratio\",\"active\":true,\"discoveredAt\":\"2018-11-28T17:01:26.000\",\"modifiedAt\":\"2018-11-29T10:27:00.000\",\"dataStore\":\"MyDataStore\",\"businessTerms\":[\"FINANTIAL\",\"RGDP\"],\"qualityRules\":[\"qualityRule2\",\"qualityRule1\"],\"keys\":[\"QUALITY\",\"OWNER\"],\"keysValues\":[\"QUALITY:=High\",\"OWNER:=finantial\"]},{\"id\":\"bt/1\",\"name\":\"financial\",\"alias\":\"\",\"description\":\"Financial Business Term\",\"metadataPath\":\"\",\"type\":\"GLOSSARY\",\"subtype\":\"BUSINESS_TERM\",\"_tenant\":\"\",\"active\":true,\"discoveredAt\":\"2018-11-28T17:01:26.000\",\"modifiedAt\":\"2018-11-28T17:01:26.000\",\"dataStore\":\"GLOSSARY\"}]"
    val chunk: Array[ElasticObject] = Array(
      new ElasticObject("da/1",Option("EmptyStore"),Option("TheOnlyOne"), Option("Empty DataStore"),"EmptyDatastore:","HDFS","DS","stratio",true, new Timestamp(milis), modifiedAt = new Timestamp(milis)),
      new ElasticObject("da/2",Option("MyDataStore"),None,Option("My DataStore"),"MyDataStore:","SQL","DS","stratio",true, new Timestamp(milis), new Timestamp(milis)),
      new ElasticObject("da/3",Option("FinantialDB"),None,Option("Finantial DataBase"),"MyDataStore://>FinantialDB/:","SQL","PATH","stratio",true, new Timestamp(milis), new Timestamp(milis)),
      new ElasticObject("da/4",Option("toys-department"),None,Option("Toys Department"),"MyDataStore://>FinantialDB/:toys-department","SQL","RESOURCE","stratio",true, new Timestamp(milis), new Timestamp(milis)),
      new ElasticObject("da/5",Option("purchaserName"),None,Option("Purchaser Name"),"MyDataStore://>FinantialDB/:toys-department:purchaserName:","SQL","FIELD","stratio",true, new Timestamp(milis), new Timestamp(milis)),
      new ElasticObject("da/6",Option("purchases"),None,Option("Purchases"),"MyDataStore://>FinantialDB/:toys-department:purchases:","SQL","FIELD","stratio",true, new Timestamp(milis), new Timestamp(milis)),
      new ElasticObject("bt/1",Option("financial"),None,Option("Financial Business Term"),"","GLOSSARY","BUSINESS_TERM","",true, new Timestamp(milis), new Timestamp(milis))
    )
    chunk.foreach( (da: ElasticObject) => {
      da.dataStore = getDataStore(da.tpe, da.metadataPath)
    })

    val result = process(chunk, noAdds = false, Some("STRING"))

    assertResult(reference)(result)

  }

  "Extractor Completed Events Simulation with empty text typed attribute" should "be processed in Indexer Mock" in {

    val milis: Long = 1543424486000l
    val reference: String = "[{\"id\":\"da/2\",\"name\":\"MyDataStore\",\"alias\":\"\",\"description\":\"My DataStore\",\"metadataPath\":\"MyDataStore:\",\"type\":\"SQL\",\"subtype\":\"DS\",\"_tenant\":\"stratio\",\"active\":true,\"discoveredAt\":\"2018-11-28T17:01:26.000\",\"modifiedAt\":\"2018-11-29T10:27:00.000\",\"dataStore\":\"MyDataStore\",\"businessTerms\":[\"FINANTIAL\",\"RGDP\"],\"qualityRules\":[\"qualityRule2\",\"qualityRule1\"],\"keys\":[\"QUALITY\",\"OWNER\"],\"keysValues\":[\"QUALITY:=\",\"OWNER:=finantial\"]},{\"id\":\"da/1\",\"name\":\"EmptyStore\",\"alias\":\"TheOnlyOne\",\"description\":\"Empty DataStore\",\"metadataPath\":\"EmptyDatastore:\",\"type\":\"HDFS\",\"subtype\":\"DS\",\"_tenant\":\"stratio\",\"active\":true,\"discoveredAt\":\"2018-11-28T17:01:26.000\",\"modifiedAt\":\"2018-11-28T17:01:26.000\",\"dataStore\":\"EmptyDatastore\"},{\"id\":\"da/3\",\"name\":\"FinantialDB\",\"alias\":\"\",\"description\":\"Finantial DataBase\",\"metadataPath\":\"MyDataStore://>FinantialDB/:\",\"type\":\"SQL\",\"subtype\":\"PATH\",\"_tenant\":\"stratio\",\"active\":true,\"discoveredAt\":\"2018-11-28T17:01:26.000\",\"modifiedAt\":\"2018-11-29T10:27:00.000\",\"dataStore\":\"MyDataStore\",\"businessTerms\":[\"FINANTIAL\",\"RGDP\"],\"qualityRules\":[\"qualityRule2\",\"qualityRule1\"],\"keys\":[\"QUALITY\",\"OWNER\"],\"keysValues\":[\"QUALITY:=\",\"OWNER:=finantial\"]},{\"id\":\"da/4\",\"name\":\"toys-department\",\"alias\":\"\",\"description\":\"Toys Department\",\"metadataPath\":\"MyDataStore://>FinantialDB/:toys-department\",\"type\":\"SQL\",\"subtype\":\"RESOURCE\",\"_tenant\":\"stratio\",\"active\":true,\"discoveredAt\":\"2018-11-28T17:01:26.000\",\"modifiedAt\":\"2018-11-29T10:27:00.000\",\"dataStore\":\"MyDataStore\",\"businessTerms\":[\"FINANTIAL\",\"RGDP\"],\"qualityRules\":[\"qualityRule2\",\"qualityRule1\"],\"keys\":[\"QUALITY\",\"OWNER\"],\"keysValues\":[\"QUALITY:=\",\"OWNER:=finantial\"]},{\"id\":\"da/5\",\"name\":\"purchaserName\",\"alias\":\"\",\"description\":\"Purchaser Name\",\"metadataPath\":\"MyDataStore://>FinantialDB/:toys-department:purchaserName:\",\"type\":\"SQL\",\"subtype\":\"FIELD\",\"_tenant\":\"stratio\",\"active\":true,\"discoveredAt\":\"2018-11-28T17:01:26.000\",\"modifiedAt\":\"2018-11-29T10:27:00.000\",\"dataStore\":\"MyDataStore\",\"businessTerms\":[\"FINANTIAL\",\"RGDP\"],\"qualityRules\":[\"qualityRule2\",\"qualityRule1\"],\"keys\":[\"QUALITY\",\"OWNER\"],\"keysValues\":[\"QUALITY:=\",\"OWNER:=finantial\"]},{\"id\":\"da/6\",\"name\":\"purchases\",\"alias\":\"\",\"description\":\"Purchases\",\"metadataPath\":\"MyDataStore://>FinantialDB/:toys-department:purchases:\",\"type\":\"SQL\",\"subtype\":\"FIELD\",\"_tenant\":\"stratio\",\"active\":true,\"discoveredAt\":\"2018-11-28T17:01:26.000\",\"modifiedAt\":\"2018-11-29T10:27:00.000\",\"dataStore\":\"MyDataStore\",\"businessTerms\":[\"FINANTIAL\",\"RGDP\"],\"qualityRules\":[\"qualityRule2\",\"qualityRule1\"],\"keys\":[\"QUALITY\",\"OWNER\"],\"keysValues\":[\"QUALITY:=\",\"OWNER:=finantial\"]},{\"id\":\"bt/1\",\"name\":\"financial\",\"alias\":\"\",\"description\":\"Financial Business Term\",\"metadataPath\":\"\",\"type\":\"GLOSSARY\",\"subtype\":\"BUSINESS_TERM\",\"_tenant\":\"\",\"active\":true,\"discoveredAt\":\"2018-11-28T17:01:26.000\",\"modifiedAt\":\"2018-11-28T17:01:26.000\",\"dataStore\":\"GLOSSARY\"}]"
    val chunk: Array[ElasticObject] = Array(
      new ElasticObject("da/1",Option("EmptyStore"),Option("TheOnlyOne"), Option("Empty DataStore"),"EmptyDatastore:","HDFS","DS","stratio",true, new Timestamp(milis), modifiedAt = new Timestamp(milis)),
      new ElasticObject("da/2",Option("MyDataStore"),None,Option("My DataStore"),"MyDataStore:","SQL","DS","stratio",true, new Timestamp(milis), new Timestamp(milis)),
      new ElasticObject("da/3",Option("FinantialDB"),None,Option("Finantial DataBase"),"MyDataStore://>FinantialDB/:","SQL","PATH","stratio",true, new Timestamp(milis), new Timestamp(milis)),
      new ElasticObject("da/4",Option("toys-department"),None,Option("Toys Department"),"MyDataStore://>FinantialDB/:toys-department","SQL","RESOURCE","stratio",true, new Timestamp(milis), new Timestamp(milis)),
      new ElasticObject("da/5",Option("purchaserName"),None,Option("Purchaser Name"),"MyDataStore://>FinantialDB/:toys-department:purchaserName:","SQL","FIELD","stratio",true, new Timestamp(milis), new Timestamp(milis)),
      new ElasticObject("da/6",Option("purchases"),None,Option("Purchases"),"MyDataStore://>FinantialDB/:toys-department:purchases:","SQL","FIELD","stratio",true, new Timestamp(milis), new Timestamp(milis)),
      new ElasticObject("bt/1",Option("financial"),None,Option("Financial Business Term"),"","GLOSSARY","BUSINESS_TERM","",true, new Timestamp(milis), new Timestamp(milis))
    )
    chunk.foreach( (da: ElasticObject) => {
      da.dataStore = getDataStore(da.tpe, da.metadataPath)
    })

    val result = process(chunk, noAdds = false, Some("EMPTY"))

    assertResult(reference)(result)

  }

  "Extractor Completed Events Simulation with null attribute" should "be processed in Indexer Mock" in {

    val milis: Long = 1543424486000l
    val reference: String = "[{\"id\":\"da/2\",\"name\":\"MyDataStore\",\"alias\":\"\",\"description\":\"My DataStore\",\"metadataPath\":\"MyDataStore:\",\"type\":\"SQL\",\"subtype\":\"DS\",\"_tenant\":\"stratio\",\"active\":true,\"discoveredAt\":\"2018-11-28T17:01:26.000\",\"modifiedAt\":\"2018-11-29T10:27:00.000\",\"dataStore\":\"MyDataStore\",\"businessTerms\":[\"FINANTIAL\",\"RGDP\"],\"qualityRules\":[\"qualityRule2\",\"qualityRule1\"],\"keys\":[\"QUALITY\",\"OWNER\"],\"keysValues\":[\"OWNER:=finantial\"]},{\"id\":\"da/1\",\"name\":\"EmptyStore\",\"alias\":\"TheOnlyOne\",\"description\":\"Empty DataStore\",\"metadataPath\":\"EmptyDatastore:\",\"type\":\"HDFS\",\"subtype\":\"DS\",\"_tenant\":\"stratio\",\"active\":true,\"discoveredAt\":\"2018-11-28T17:01:26.000\",\"modifiedAt\":\"2018-11-28T17:01:26.000\",\"dataStore\":\"EmptyDatastore\"},{\"id\":\"da/3\",\"name\":\"FinantialDB\",\"alias\":\"\",\"description\":\"Finantial DataBase\",\"metadataPath\":\"MyDataStore://>FinantialDB/:\",\"type\":\"SQL\",\"subtype\":\"PATH\",\"_tenant\":\"stratio\",\"active\":true,\"discoveredAt\":\"2018-11-28T17:01:26.000\",\"modifiedAt\":\"2018-11-29T10:27:00.000\",\"dataStore\":\"MyDataStore\",\"businessTerms\":[\"FINANTIAL\",\"RGDP\"],\"qualityRules\":[\"qualityRule2\",\"qualityRule1\"],\"keys\":[\"QUALITY\",\"OWNER\"],\"keysValues\":[\"OWNER:=finantial\"]},{\"id\":\"da/4\",\"name\":\"toys-department\",\"alias\":\"\",\"description\":\"Toys Department\",\"metadataPath\":\"MyDataStore://>FinantialDB/:toys-department\",\"type\":\"SQL\",\"subtype\":\"RESOURCE\",\"_tenant\":\"stratio\",\"active\":true,\"discoveredAt\":\"2018-11-28T17:01:26.000\",\"modifiedAt\":\"2018-11-29T10:27:00.000\",\"dataStore\":\"MyDataStore\",\"businessTerms\":[\"FINANTIAL\",\"RGDP\"],\"qualityRules\":[\"qualityRule2\",\"qualityRule1\"],\"keys\":[\"QUALITY\",\"OWNER\"],\"keysValues\":[\"OWNER:=finantial\"]},{\"id\":\"da/5\",\"name\":\"purchaserName\",\"alias\":\"\",\"description\":\"Purchaser Name\",\"metadataPath\":\"MyDataStore://>FinantialDB/:toys-department:purchaserName:\",\"type\":\"SQL\",\"subtype\":\"FIELD\",\"_tenant\":\"stratio\",\"active\":true,\"discoveredAt\":\"2018-11-28T17:01:26.000\",\"modifiedAt\":\"2018-11-29T10:27:00.000\",\"dataStore\":\"MyDataStore\",\"businessTerms\":[\"FINANTIAL\",\"RGDP\"],\"qualityRules\":[\"qualityRule2\",\"qualityRule1\"],\"keys\":[\"QUALITY\",\"OWNER\"],\"keysValues\":[\"OWNER:=finantial\"]},{\"id\":\"da/6\",\"name\":\"purchases\",\"alias\":\"\",\"description\":\"Purchases\",\"metadataPath\":\"MyDataStore://>FinantialDB/:toys-department:purchases:\",\"type\":\"SQL\",\"subtype\":\"FIELD\",\"_tenant\":\"stratio\",\"active\":true,\"discoveredAt\":\"2018-11-28T17:01:26.000\",\"modifiedAt\":\"2018-11-29T10:27:00.000\",\"dataStore\":\"MyDataStore\",\"businessTerms\":[\"FINANTIAL\",\"RGDP\"],\"qualityRules\":[\"qualityRule2\",\"qualityRule1\"],\"keys\":[\"QUALITY\",\"OWNER\"],\"keysValues\":[\"OWNER:=finantial\"]},{\"id\":\"bt/1\",\"name\":\"financial\",\"alias\":\"\",\"description\":\"Financial Business Term\",\"metadataPath\":\"\",\"type\":\"GLOSSARY\",\"subtype\":\"BUSINESS_TERM\",\"_tenant\":\"\",\"active\":true,\"discoveredAt\":\"2018-11-28T17:01:26.000\",\"modifiedAt\":\"2018-11-28T17:01:26.000\",\"dataStore\":\"GLOSSARY\"}]"
    val chunk: Array[ElasticObject] = Array(
      new ElasticObject("da/1",Option("EmptyStore"),Option("TheOnlyOne"), Option("Empty DataStore"),"EmptyDatastore:","HDFS","DS","stratio",true, new Timestamp(milis), modifiedAt = new Timestamp(milis)),
      new ElasticObject("da/2",Option("MyDataStore"),None,Option("My DataStore"),"MyDataStore:","SQL","DS","stratio",true, new Timestamp(milis), new Timestamp(milis)),
      new ElasticObject("da/3",Option("FinantialDB"),None,Option("Finantial DataBase"),"MyDataStore://>FinantialDB/:","SQL","PATH","stratio",true, new Timestamp(milis), new Timestamp(milis)),
      new ElasticObject("da/4",Option("toys-department"),None,Option("Toys Department"),"MyDataStore://>FinantialDB/:toys-department","SQL","RESOURCE","stratio",true, new Timestamp(milis), new Timestamp(milis)),
      new ElasticObject("da/5",Option("purchaserName"),None,Option("Purchaser Name"),"MyDataStore://>FinantialDB/:toys-department:purchaserName:","SQL","FIELD","stratio",true, new Timestamp(milis), new Timestamp(milis)),
      new ElasticObject("da/6",Option("purchases"),None,Option("Purchases"),"MyDataStore://>FinantialDB/:toys-department:purchases:","SQL","FIELD","stratio",true, new Timestamp(milis), new Timestamp(milis)),
      new ElasticObject("bt/1",Option("financial"),None,Option("Financial Business Term"),"","GLOSSARY","BUSINESS_TERM","",true, new Timestamp(milis), new Timestamp(milis))
    )
    chunk.foreach( (da: ElasticObject) => {
      da.dataStore = getDataStore(da.tpe, da.metadataPath)
    })

    val result = process(chunk, noAdds = false, Some("NULL"))

    assertResult(reference)(result)

  }

  "Extractor Completed Events Simulation with boolean typed attribute" should "be processed in Indexer Mock" in {

    val milis: Long = 1543424486000l
    val reference: String = "[{\"id\":\"da/2\",\"name\":\"MyDataStore\",\"alias\":\"\",\"description\":\"My DataStore\",\"metadataPath\":\"MyDataStore:\",\"type\":\"SQL\",\"subtype\":\"DS\",\"_tenant\":\"stratio\",\"active\":true,\"discoveredAt\":\"2018-11-28T17:01:26.000\",\"modifiedAt\":\"2018-11-29T10:27:00.000\",\"dataStore\":\"MyDataStore\",\"businessTerms\":[\"FINANTIAL\",\"RGDP\"],\"qualityRules\":[\"qualityRule2\",\"qualityRule1\"],\"keys\":[\"QUALITY\",\"OWNER\"],\"keysValues\":[\"QUALITY:=true\",\"OWNER:=finantial\"]},{\"id\":\"da/1\",\"name\":\"EmptyStore\",\"alias\":\"TheOnlyOne\",\"description\":\"Empty DataStore\",\"metadataPath\":\"EmptyDatastore:\",\"type\":\"HDFS\",\"subtype\":\"DS\",\"_tenant\":\"stratio\",\"active\":true,\"discoveredAt\":\"2018-11-28T17:01:26.000\",\"modifiedAt\":\"2018-11-28T17:01:26.000\",\"dataStore\":\"EmptyDatastore\"},{\"id\":\"da/3\",\"name\":\"FinantialDB\",\"alias\":\"\",\"description\":\"Finantial DataBase\",\"metadataPath\":\"MyDataStore://>FinantialDB/:\",\"type\":\"SQL\",\"subtype\":\"PATH\",\"_tenant\":\"stratio\",\"active\":true,\"discoveredAt\":\"2018-11-28T17:01:26.000\",\"modifiedAt\":\"2018-11-29T10:27:00.000\",\"dataStore\":\"MyDataStore\",\"businessTerms\":[\"FINANTIAL\",\"RGDP\"],\"qualityRules\":[\"qualityRule2\",\"qualityRule1\"],\"keys\":[\"QUALITY\",\"OWNER\"],\"keysValues\":[\"QUALITY:=true\",\"OWNER:=finantial\"]},{\"id\":\"da/4\",\"name\":\"toys-department\",\"alias\":\"\",\"description\":\"Toys Department\",\"metadataPath\":\"MyDataStore://>FinantialDB/:toys-department\",\"type\":\"SQL\",\"subtype\":\"RESOURCE\",\"_tenant\":\"stratio\",\"active\":true,\"discoveredAt\":\"2018-11-28T17:01:26.000\",\"modifiedAt\":\"2018-11-29T10:27:00.000\",\"dataStore\":\"MyDataStore\",\"businessTerms\":[\"FINANTIAL\",\"RGDP\"],\"qualityRules\":[\"qualityRule2\",\"qualityRule1\"],\"keys\":[\"QUALITY\",\"OWNER\"],\"keysValues\":[\"QUALITY:=true\",\"OWNER:=finantial\"]},{\"id\":\"da/5\",\"name\":\"purchaserName\",\"alias\":\"\",\"description\":\"Purchaser Name\",\"metadataPath\":\"MyDataStore://>FinantialDB/:toys-department:purchaserName:\",\"type\":\"SQL\",\"subtype\":\"FIELD\",\"_tenant\":\"stratio\",\"active\":true,\"discoveredAt\":\"2018-11-28T17:01:26.000\",\"modifiedAt\":\"2018-11-29T10:27:00.000\",\"dataStore\":\"MyDataStore\",\"businessTerms\":[\"FINANTIAL\",\"RGDP\"],\"qualityRules\":[\"qualityRule2\",\"qualityRule1\"],\"keys\":[\"QUALITY\",\"OWNER\"],\"keysValues\":[\"QUALITY:=true\",\"OWNER:=finantial\"]},{\"id\":\"da/6\",\"name\":\"purchases\",\"alias\":\"\",\"description\":\"Purchases\",\"metadataPath\":\"MyDataStore://>FinantialDB/:toys-department:purchases:\",\"type\":\"SQL\",\"subtype\":\"FIELD\",\"_tenant\":\"stratio\",\"active\":true,\"discoveredAt\":\"2018-11-28T17:01:26.000\",\"modifiedAt\":\"2018-11-29T10:27:00.000\",\"dataStore\":\"MyDataStore\",\"businessTerms\":[\"FINANTIAL\",\"RGDP\"],\"qualityRules\":[\"qualityRule2\",\"qualityRule1\"],\"keys\":[\"QUALITY\",\"OWNER\"],\"keysValues\":[\"QUALITY:=true\",\"OWNER:=finantial\"]},{\"id\":\"bt/1\",\"name\":\"financial\",\"alias\":\"\",\"description\":\"Financial Business Term\",\"metadataPath\":\"\",\"type\":\"GLOSSARY\",\"subtype\":\"BUSINESS_TERM\",\"_tenant\":\"\",\"active\":true,\"discoveredAt\":\"2018-11-28T17:01:26.000\",\"modifiedAt\":\"2018-11-28T17:01:26.000\",\"dataStore\":\"GLOSSARY\"}]"
    val chunk: Array[ElasticObject] = Array(
      new ElasticObject("da/1",Option("EmptyStore"),Option("TheOnlyOne"), Option("Empty DataStore"),"EmptyDatastore:","HDFS","DS","stratio",true, new Timestamp(milis), modifiedAt = new Timestamp(milis)),
      new ElasticObject("da/2",Option("MyDataStore"),None,Option("My DataStore"),"MyDataStore:","SQL","DS","stratio",true, new Timestamp(milis), new Timestamp(milis)),
      new ElasticObject("da/3",Option("FinantialDB"),None,Option("Finantial DataBase"),"MyDataStore://>FinantialDB/:","SQL","PATH","stratio",true, new Timestamp(milis), new Timestamp(milis)),
      new ElasticObject("da/4",Option("toys-department"),None,Option("Toys Department"),"MyDataStore://>FinantialDB/:toys-department","SQL","RESOURCE","stratio",true, new Timestamp(milis), new Timestamp(milis)),
      new ElasticObject("da/5",Option("purchaserName"),None,Option("Purchaser Name"),"MyDataStore://>FinantialDB/:toys-department:purchaserName:","SQL","FIELD","stratio",true, new Timestamp(milis), new Timestamp(milis)),
      new ElasticObject("da/6",Option("purchases"),None,Option("Purchases"),"MyDataStore://>FinantialDB/:toys-department:purchases:","SQL","FIELD","stratio",true, new Timestamp(milis), new Timestamp(milis)),
      new ElasticObject("bt/1",Option("financial"),None,Option("Financial Business Term"),"","GLOSSARY","BUSINESS_TERM","",true, new Timestamp(milis), new Timestamp(milis))
    )
    chunk.foreach( (da: ElasticObject) => {
      da.dataStore = getDataStore(da.tpe, da.metadataPath)
    })

    val result = process(chunk, noAdds = false, Some("BOOLEAN"))

    assertResult(reference)(result)

  }

  "Extractor Completed Events Simulation with number typed attribute" should "be processed in Indexer Mock" in {

    val milis: Long = 1543424486000l
    val reference: String = "[{\"id\":\"da/2\",\"name\":\"MyDataStore\",\"alias\":\"\",\"description\":\"My DataStore\",\"metadataPath\":\"MyDataStore:\",\"type\":\"SQL\",\"subtype\":\"DS\",\"_tenant\":\"stratio\",\"active\":true,\"discoveredAt\":\"2018-11-28T17:01:26.000\",\"modifiedAt\":\"2018-11-29T10:27:00.000\",\"dataStore\":\"MyDataStore\",\"businessTerms\":[\"FINANTIAL\",\"RGDP\"],\"qualityRules\":[\"qualityRule2\",\"qualityRule1\"],\"keys\":[\"QUALITY\",\"OWNER\"],\"keysValues\":[\"QUALITY:=1234\",\"OWNER:=finantial\"]},{\"id\":\"da/1\",\"name\":\"EmptyStore\",\"alias\":\"TheOnlyOne\",\"description\":\"Empty DataStore\",\"metadataPath\":\"EmptyDatastore:\",\"type\":\"HDFS\",\"subtype\":\"DS\",\"_tenant\":\"stratio\",\"active\":true,\"discoveredAt\":\"2018-11-28T17:01:26.000\",\"modifiedAt\":\"2018-11-28T17:01:26.000\",\"dataStore\":\"EmptyDatastore\"},{\"id\":\"da/3\",\"name\":\"FinantialDB\",\"alias\":\"\",\"description\":\"Finantial DataBase\",\"metadataPath\":\"MyDataStore://>FinantialDB/:\",\"type\":\"SQL\",\"subtype\":\"PATH\",\"_tenant\":\"stratio\",\"active\":true,\"discoveredAt\":\"2018-11-28T17:01:26.000\",\"modifiedAt\":\"2018-11-29T10:27:00.000\",\"dataStore\":\"MyDataStore\",\"businessTerms\":[\"FINANTIAL\",\"RGDP\"],\"qualityRules\":[\"qualityRule2\",\"qualityRule1\"],\"keys\":[\"QUALITY\",\"OWNER\"],\"keysValues\":[\"QUALITY:=1234\",\"OWNER:=finantial\"]},{\"id\":\"da/4\",\"name\":\"toys-department\",\"alias\":\"\",\"description\":\"Toys Department\",\"metadataPath\":\"MyDataStore://>FinantialDB/:toys-department\",\"type\":\"SQL\",\"subtype\":\"RESOURCE\",\"_tenant\":\"stratio\",\"active\":true,\"discoveredAt\":\"2018-11-28T17:01:26.000\",\"modifiedAt\":\"2018-11-29T10:27:00.000\",\"dataStore\":\"MyDataStore\",\"businessTerms\":[\"FINANTIAL\",\"RGDP\"],\"qualityRules\":[\"qualityRule2\",\"qualityRule1\"],\"keys\":[\"QUALITY\",\"OWNER\"],\"keysValues\":[\"QUALITY:=1234\",\"OWNER:=finantial\"]},{\"id\":\"da/5\",\"name\":\"purchaserName\",\"alias\":\"\",\"description\":\"Purchaser Name\",\"metadataPath\":\"MyDataStore://>FinantialDB/:toys-department:purchaserName:\",\"type\":\"SQL\",\"subtype\":\"FIELD\",\"_tenant\":\"stratio\",\"active\":true,\"discoveredAt\":\"2018-11-28T17:01:26.000\",\"modifiedAt\":\"2018-11-29T10:27:00.000\",\"dataStore\":\"MyDataStore\",\"businessTerms\":[\"FINANTIAL\",\"RGDP\"],\"qualityRules\":[\"qualityRule2\",\"qualityRule1\"],\"keys\":[\"QUALITY\",\"OWNER\"],\"keysValues\":[\"QUALITY:=1234\",\"OWNER:=finantial\"]},{\"id\":\"da/6\",\"name\":\"purchases\",\"alias\":\"\",\"description\":\"Purchases\",\"metadataPath\":\"MyDataStore://>FinantialDB/:toys-department:purchases:\",\"type\":\"SQL\",\"subtype\":\"FIELD\",\"_tenant\":\"stratio\",\"active\":true,\"discoveredAt\":\"2018-11-28T17:01:26.000\",\"modifiedAt\":\"2018-11-29T10:27:00.000\",\"dataStore\":\"MyDataStore\",\"businessTerms\":[\"FINANTIAL\",\"RGDP\"],\"qualityRules\":[\"qualityRule2\",\"qualityRule1\"],\"keys\":[\"QUALITY\",\"OWNER\"],\"keysValues\":[\"QUALITY:=1234\",\"OWNER:=finantial\"]},{\"id\":\"bt/1\",\"name\":\"financial\",\"alias\":\"\",\"description\":\"Financial Business Term\",\"metadataPath\":\"\",\"type\":\"GLOSSARY\",\"subtype\":\"BUSINESS_TERM\",\"_tenant\":\"\",\"active\":true,\"discoveredAt\":\"2018-11-28T17:01:26.000\",\"modifiedAt\":\"2018-11-28T17:01:26.000\",\"dataStore\":\"GLOSSARY\"}]"
    val chunk: Array[ElasticObject] = Array(
      new ElasticObject("da/1",Option("EmptyStore"),Option("TheOnlyOne"), Option("Empty DataStore"),"EmptyDatastore:","HDFS","DS","stratio",true, new Timestamp(milis), modifiedAt = new Timestamp(milis)),
      new ElasticObject("da/2",Option("MyDataStore"),None,Option("My DataStore"),"MyDataStore:","SQL","DS","stratio",true, new Timestamp(milis), new Timestamp(milis)),
      new ElasticObject("da/3",Option("FinantialDB"),None,Option("Finantial DataBase"),"MyDataStore://>FinantialDB/:","SQL","PATH","stratio",true, new Timestamp(milis), new Timestamp(milis)),
      new ElasticObject("da/4",Option("toys-department"),None,Option("Toys Department"),"MyDataStore://>FinantialDB/:toys-department","SQL","RESOURCE","stratio",true, new Timestamp(milis), new Timestamp(milis)),
      new ElasticObject("da/5",Option("purchaserName"),None,Option("Purchaser Name"),"MyDataStore://>FinantialDB/:toys-department:purchaserName:","SQL","FIELD","stratio",true, new Timestamp(milis), new Timestamp(milis)),
      new ElasticObject("da/6",Option("purchases"),None,Option("Purchases"),"MyDataStore://>FinantialDB/:toys-department:purchases:","SQL","FIELD","stratio",true, new Timestamp(milis), new Timestamp(milis)),
      new ElasticObject("bt/1",Option("financial"),None,Option("Financial Business Term"),"","GLOSSARY","BUSINESS_TERM","",true, new Timestamp(milis), new Timestamp(milis))
    )
    chunk.foreach( (da: ElasticObject) => {
      da.dataStore = getDataStore(da.tpe, da.metadataPath)
    })

    val result = process(chunk, noAdds = false, Some("NUMBER"))

    assertResult(reference)(result)

  }

  "Extractor No Additionals Events Simulation" should "be processed in Indexer Mock" in {

    val milis: Long = 1543424486000l
    val reference: String = "[{\"id\":\"da/1\",\"name\":\"EmptyStore\",\"alias\":\"\",\"description\":\"Empty DataStore\",\"metadataPath\":\"EmptyDatastore:\",\"type\":\"HDFS\",\"subtype\":\"DS\",\"_tenant\":\"stratio\",\"active\":true,\"discoveredAt\":\"2018-11-28T17:01:26.000\",\"modifiedAt\":\"2018-11-28T17:01:26.000\",\"dataStore\":\"EmptyDatastore\"},{\"id\":\"da/2\",\"name\":\"MyDataStore\",\"alias\":\"\",\"description\":\"My DataStore\",\"metadataPath\":\"MyDataStore:\",\"type\":\"SQL\",\"subtype\":\"DS\",\"_tenant\":\"stratio\",\"active\":true,\"discoveredAt\":\"2018-11-28T17:01:26.000\",\"modifiedAt\":\"2018-11-28T17:01:26.000\",\"dataStore\":\"MyDataStore\"},{\"id\":\"da/3\",\"name\":\"FinantialDB\",\"alias\":\"\",\"description\":\"Finantial DataBase\",\"metadataPath\":\"MyDataStore://>FinantialDB/:\",\"type\":\"SQL\",\"subtype\":\"PATH\",\"_tenant\":\"stratio\",\"active\":true,\"discoveredAt\":\"2018-11-28T17:01:26.000\",\"modifiedAt\":\"2018-11-28T17:01:26.000\",\"dataStore\":\"MyDataStore\"},{\"id\":\"da/4\",\"name\":\"toys-department\",\"alias\":\"\",\"description\":\"Toys Department\",\"metadataPath\":\"MyDataStore://>FinantialDB/:toys-department\",\"type\":\"SQL\",\"subtype\":\"RESOURCE\",\"_tenant\":\"stratio\",\"active\":true,\"discoveredAt\":\"2018-11-28T17:01:26.000\",\"modifiedAt\":\"2018-11-28T17:01:26.000\",\"dataStore\":\"MyDataStore\"},{\"id\":\"da/5\",\"name\":\"purchaserName\",\"alias\":\"\",\"description\":\"Purchaser Name\",\"metadataPath\":\"MyDataStore://>FinantialDB/:toys-department:purchaserName:\",\"type\":\"SQL\",\"subtype\":\"FIELD\",\"_tenant\":\"stratio\",\"active\":true,\"discoveredAt\":\"2018-11-28T17:01:26.000\",\"modifiedAt\":\"2018-11-28T17:01:26.000\",\"dataStore\":\"MyDataStore\"},{\"id\":\"da/6\",\"name\":\"purchases\",\"alias\":\"\",\"description\":\"Purchases\",\"metadataPath\":\"MyDataStore://>FinantialDB/:toys-department:purchases:\",\"type\":\"SQL\",\"subtype\":\"FIELD\",\"_tenant\":\"stratio\",\"active\":true,\"discoveredAt\":\"2018-11-28T17:01:26.000\",\"modifiedAt\":\"2018-11-28T17:01:26.000\",\"dataStore\":\"MyDataStore\"},{\"id\":\"bt/1\",\"name\":\"financial\",\"alias\":\"\",\"description\":\"Financial Business Term\",\"metadataPath\":\"\",\"type\":\"GLOSSARY\",\"subtype\":\"BUSINESS_TERM\",\"_tenant\":\"\",\"active\":true,\"discoveredAt\":\"2018-11-28T17:01:26.000\",\"modifiedAt\":\"2018-11-28T17:01:26.000\",\"dataStore\":\"GLOSSARY\"}]"
    val chunk: Array[ElasticObject] = Array(
      new ElasticObject("da/1",Option("EmptyStore"),None,Option("Empty DataStore"),"EmptyDatastore:","HDFS","DS","stratio",true, new Timestamp(milis), new Timestamp(milis)),
      new ElasticObject("da/2",Option("MyDataStore"),None,Option("My DataStore"),"MyDataStore:","SQL","DS","stratio", true, new Timestamp(milis), new Timestamp(milis)),
      new ElasticObject("da/3",Option("FinantialDB"),None,Option("Finantial DataBase"),"MyDataStore://>FinantialDB/:","SQL","PATH","stratio",true, new Timestamp(milis), new Timestamp(milis)),
      new ElasticObject("da/4",Option("toys-department"),None,Option("Toys Department"),"MyDataStore://>FinantialDB/:toys-department","SQL","RESOURCE","stratio",true, new Timestamp(milis), new Timestamp(milis)),
      new ElasticObject("da/5",Option("purchaserName"),None,Option("Purchaser Name"),"MyDataStore://>FinantialDB/:toys-department:purchaserName:","SQL","FIELD","stratio",true, new Timestamp(milis), new Timestamp(milis)),
      new ElasticObject("da/6",Option("purchases"),None,Option("Purchases"),"MyDataStore://>FinantialDB/:toys-department:purchases:","SQL","FIELD","stratio",true, new Timestamp(milis), new Timestamp(milis)),
      new ElasticObject("bt/1",Option("financial"),None,Option("Financial Business Term"),"","GLOSSARY","BUSINESS_TERM","",true, new Timestamp(milis), new Timestamp(milis))
    )
    chunk.foreach( (da: ElasticObject) => {
      da.dataStore = getDataStore(da.tpe, da.metadataPath)
    })

    val result = process(chunk, noAdds = true, None)

    assertResult(reference)(result)

  }

  "Extractor DGPB-XXXX Events Simulation" should "be processed in Indexer Mock" in {

    val milis: Long = 1543424486000l
    val reference: String = "[{\"id\":\"da/2\",\"name\":\"MyDataStore\",\"alias\":\"\",\"description\":\"My DataStore\",\"metadataPath\":\"MyDataStore:\",\"type\":\"SQL\",\"subtype\":\"DS\",\"_tenant\":\"stratio\",\"active\":true,\"discoveredAt\":\"2018-11-28T17:01:26.000\",\"modifiedAt\":\"2018-11-29T10:27:00.000\",\"dataStore\":\"MyDataStore\",\"businessTerms\":[\"FINANTIAL\",\"RGDP\"],\"qualityRules\":[\"qualityRule2\",\"qualityRule1\"],\"keys\":[\"QUALITY\",\"OWNER\"],\"keysValues\":[\"QUALITY:=High\",\"OWNER:=finantial\"]},{\"id\":\"bt/2\",\"name\":\"financial\",\"alias\":\"\",\"description\":\"Financial Business Term\",\"metadataPath\":\"\",\"type\":\"GLOSSARY\",\"subtype\":\"BUSINESS_TERMS\",\"_tenant\":\"\",\"active\":true,\"discoveredAt\":\"2018-11-28T17:01:26.000\",\"modifiedAt\":\"2018-11-28T17:01:26.000\",\"dataStore\":\"GLOSSARY\"}]"
    val chunk: Array[ElasticObject] = Array(
      new ElasticObject("da/2",Option("MyDataStore"),None,Option("My DataStore"),"MyDataStore:","SQL","DS","stratio",true, new Timestamp(milis), new Timestamp(milis)),
      new ElasticObject("bt/2",Option("financial"),None,Option("Financial Business Term"),"","GLOSSARY","BUSINESS_TERMS","",true, new Timestamp(milis), new Timestamp(milis))
    )
    chunk.foreach( (da: ElasticObject) => {
      da.dataStore = getDataStore(da.tpe, da.metadataPath)
    })

    val result = process(chunk, noAdds = false, Some("STRING"))

    assertResult(reference)(result)

  }

  private def process(chunk: Array[ElasticObject], noAdds: Boolean, value_type: Option[String]): String = {
    val s: Semaphore = new Semaphore(1)
    //
    val sourceDao: CustomTestSourceDao = new CustomTestSourceDao(noAdds, value_type)
    val eParams: ExtractorTestParams = new ExtractorTestParams(s, sourceDao, chunk)
    val piParams: PartialIndexerTestParams = new PartialIndexerTestParams(s, noAdds, value_type)

    eParams.getSemaphore.acquire()

    val actorSystem: SearcherActorSystem[SASTExtractor, DGIndexer] = new SearcherActorSystem[SASTExtractor, DGIndexer]("test", classOf[SASTExtractor], classOf[DGIndexer], eParams, piParams)
    actorSystem.initPartialIndexation()

    eParams.getSemaphore.acquire()
    eParams.getSemaphore.release()

    actorSystem.stopAll()
    piParams.getResult
  }

  private def getDataStore(typ: String, metadataPath: String): String = {
    typ match {
      case "GLOSSARY" =>
        "GLOSSARY"
      case _ =>
        try {
          metadataPath.substring(0, metadataPath.indexOf(":"))
        } catch {
          case e: Throwable => {
            ""
          }
        }
    }
  }
}