package com.stratio.governance.agent.searcher.actors.indexer

import java.sql.{PreparedStatement, ResultSet, Timestamp}
import java.util.concurrent.Semaphore

import akka.actor.{Actor, ActorRef, Cancellable}
import com.stratio.governance.agent.searcher.actors.SearcherActorSystem
import com.stratio.governance.agent.searcher.actors.dao.postgres.PostgresPartialIndexationReadState
import com.stratio.governance.agent.searcher.actors.extractor.DGExtractorParams
import com.stratio.governance.agent.searcher.actors.extractor.dao.{SourceDao => ExtractorSourceDao}
import com.stratio.governance.agent.searcher.actors.indexer.dao.{SearcherDao, SourceDao => IndexerSourceDao}
import com.stratio.governance.agent.searcher.model.es.DataAssetES
import com.stratio.governance.agent.searcher.model.utils.ExponentialBackOff
import com.stratio.governance.agent.searcher.model.{BusinessAsset, KeyValuePair}
import org.scalatest.FlatSpec
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class ExtractorTestParams(s: Semaphore, sourceDao: CustomTestSourceDao, chunk: Array[DataAssetES]) extends DGExtractorParams(sourceDao, 10,10, ExponentialBackOff(10, 10),10,"test") {

  def getSemaphore: Semaphore = {
    s
  }

  def getChunk: Array[DataAssetES] = {
    chunk
  }

}
class CustomTestSourceDao(noAdds: Boolean) extends ExtractorSourceDao with IndexerSourceDao {
  override def keyValuePairProcess(ids: Array[Int]): List[KeyValuePair] = {
    if (!noAdds) {
      val rows: List[List[KeyValuePair]] = ids.map(i => List(KeyValuePair(i, "OWNER", "finantial", "2018-11-29T10:27:00.000"), KeyValuePair(i, "QUALITY", "High", "2018-09-28T20:45:00.000"))).toList
      rows.fold[List[KeyValuePair]](List())((a: List[KeyValuePair], b: List[KeyValuePair]) => {
        a ++ b
      }).filter(a => a.getId != 1)
    } else {
      List[KeyValuePair]()
    }
  }

  override def businessAssets(ids: Array[Int]): List[BusinessAsset] = {
    if (!noAdds) {
      val rows: List[List[BusinessAsset]] = ids.map(i => List(BusinessAsset(i, "RGDP", "RGDP law","APR","TERM","2018-09-28T20:45:00.000"), BusinessAsset(i, "FINANTIAL", "FINANTIAL law","APR","TERM" , "2018-09-28T20:45:00.000"))).toList
      rows.fold[List[BusinessAsset]](List())((a: List[BusinessAsset], b: List[BusinessAsset]) => {
        a ++ b
      }).filter(a => a.getId != 1)
    } else {
      List[BusinessAsset]()
    }
  }

  override def close(): Unit = ???

  override def readDataAssetsSince(offset: Int, limit: Int): (Array[DataAssetES], Int) = ???

  override def readDataAssetsWhereIdsIn(ids: List[Int]): Array[DataAssetES] = ???

  override def readUpdatedDataAssetsIdsSince(state: PostgresPartialIndexationReadState): (List[Int], PostgresPartialIndexationReadState) = ???

  override def readPartialIndexationState(): PostgresPartialIndexationReadState = ???

  override def writePartialIndexationState(state: PostgresPartialIndexationReadState): Unit = ???

  override def prepareStatement(queryName: String): PreparedStatement = ???

  override def executeQuery(sql: String): ResultSet = ???

  override def executePreparedStatement(sql: PreparedStatement): ResultSet = ???
}

class PartialIndexerTestParams(s: Semaphore, noAdds: Boolean) extends IndexerParams {

  var result: String = ""

  def getSemaphore: Semaphore = {
    s
  }

  override def getSourceDao: CustomTestSourceDao = new CustomTestSourceDao(noAdds)

  override def getSearcherDao: SearcherDao = new SearcherDao {

    override def indexPartial(model: String, doc: String): Unit = {
      result = doc
      s.release()
    }

    override def indexTotal(model: String, doc: String, token: String): Unit = {
      result = doc
      s.release()
    }
  }

  def getResult: String = {
    result
  }

  override def getPartition: Int = {
    2
  }
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

class DGIndexerTest extends FlatSpec {

  "Extractor Completed Events Simulation" should "be processed in Indexer Mock" in {

    val milis: Long = 1543424486000l
    val reference: String = "[{\"id\":2,\"name\":\"MyDataStore\",\"description\":\"My DataStore\",\"metadataPath\":\"MyDataStore:\",\"type\":\"SQL\",\"subtype\":\"DS\",\"tenant\":\"stratio\",\"active\":true,\"discoveredAt\":\"2018-11-28T18:01:26.000\",\"modifiedAt\":\"2018-11-29T10:27:00.000\",\"dataStore\":\"MyDataStore\",\"businessTerms\":[\"FINANTIAL\",\"RGDP\"],\"keys\":[\"QUALITY\",\"OWNER\"],\"key.QUALITY\":\"High\",\"key.OWNER\":\"finantial\"},{\"id\":1,\"name\":\"EmptyStore\",\"description\":\"Empty DataStore\",\"metadataPath\":\"EmptyDatastore:\",\"type\":\"HDFS\",\"subtype\":\"DS\",\"tenant\":\"stratio\",\"active\":true,\"discoveredAt\":\"2018-11-28T18:01:26.000\",\"modifiedAt\":\"2018-11-28T18:01:26.000\",\"dataStore\":\"EmptyDatastore\"},{\"id\":3,\"name\":\"FinantialDB\",\"description\":\"Finantial DataBase\",\"metadataPath\":\"MyDataStore://>FinantialDB/:\",\"type\":\"SQL\",\"subtype\":\"PATH\",\"tenant\":\"stratio\",\"active\":true,\"discoveredAt\":\"2018-11-28T18:01:26.000\",\"modifiedAt\":\"2018-11-29T10:27:00.000\",\"dataStore\":\"MyDataStore\",\"businessTerms\":[\"FINANTIAL\",\"RGDP\"],\"keys\":[\"QUALITY\",\"OWNER\"],\"key.QUALITY\":\"High\",\"key.OWNER\":\"finantial\"},{\"id\":4,\"name\":\"toys-department\",\"description\":\"Toys Department\",\"metadataPath\":\"MyDataStore://>FinantialDB/:toys-department\",\"type\":\"SQL\",\"subtype\":\"RESOURCE\",\"tenant\":\"stratio\",\"active\":true,\"discoveredAt\":\"2018-11-28T18:01:26.000\",\"modifiedAt\":\"2018-11-29T10:27:00.000\",\"dataStore\":\"MyDataStore\",\"businessTerms\":[\"FINANTIAL\",\"RGDP\"],\"keys\":[\"QUALITY\",\"OWNER\"],\"key.QUALITY\":\"High\",\"key.OWNER\":\"finantial\"},{\"id\":5,\"name\":\"purchaserName\",\"description\":\"Purchaser Name\",\"metadataPath\":\"MyDataStore://>FinantialDB/:toys-department:purchaserName:\",\"type\":\"SQL\",\"subtype\":\"FIELD\",\"tenant\":\"stratio\",\"active\":true,\"discoveredAt\":\"2018-11-28T18:01:26.000\",\"modifiedAt\":\"2018-11-29T10:27:00.000\",\"dataStore\":\"MyDataStore\",\"businessTerms\":[\"FINANTIAL\",\"RGDP\"],\"keys\":[\"QUALITY\",\"OWNER\"],\"key.QUALITY\":\"High\",\"key.OWNER\":\"finantial\"},{\"id\":6,\"name\":\"purchases\",\"description\":\"Purchases\",\"metadataPath\":\"MyDataStore://>FinantialDB/:toys-department:purchases:\",\"type\":\"SQL\",\"subtype\":\"FIELD\",\"tenant\":\"stratio\",\"active\":true,\"discoveredAt\":\"2018-11-28T18:01:26.000\",\"modifiedAt\":\"2018-11-29T10:27:00.000\",\"dataStore\":\"MyDataStore\",\"businessTerms\":[\"FINANTIAL\",\"RGDP\"],\"keys\":[\"QUALITY\",\"OWNER\"],\"key.QUALITY\":\"High\",\"key.OWNER\":\"finantial\"}]"
    val chunk: Array[DataAssetES] = Array(
      new DataAssetES(1,Option("EmptyStore"),Option("Empty DataStore"),"EmptyDatastore:","HDFS","DS","stratio",true, new Timestamp(milis), modifiedAt = new Timestamp(milis)),
      new DataAssetES(2,Option("MyDataStore"),Option("My DataStore"),"MyDataStore:","SQL","DS","stratio",true, new Timestamp(milis), new Timestamp(milis)),
      new DataAssetES(3,Option("FinantialDB"),Option("Finantial DataBase"),"MyDataStore://>FinantialDB/:","SQL","PATH","stratio",true, new Timestamp(milis), new Timestamp(milis)),
      new DataAssetES(4,Option("toys-department"),Option("Toys Department"),"MyDataStore://>FinantialDB/:toys-department","SQL","RESOURCE","stratio",true, new Timestamp(milis), new Timestamp(milis)),
      new DataAssetES(5,Option("purchaserName"),Option("Purchaser Name"),"MyDataStore://>FinantialDB/:toys-department:purchaserName:","SQL","FIELD","stratio",true, new Timestamp(milis), new Timestamp(milis)),
      new DataAssetES(6,Option("purchases"),Option("Purchases"),"MyDataStore://>FinantialDB/:toys-department:purchases:","SQL","FIELD","stratio",true, new Timestamp(milis), new Timestamp(milis))
    )

    val result = process(chunk, noAdds = false)

    assertResult(reference)(result)

  }

  "Extractor No Additionals Events Simulation" should "be processed in Indexer Mock" in {

    val milis: Long = 1543424486000l
      val reference: String = "[{\"id\":1,\"name\":\"EmptyStore\",\"description\":\"Empty DataStore\",\"metadataPath\":\"EmptyDatastore:\",\"type\":\"HDFS\",\"subtype\":\"DS\",\"tenant\":\"stratio\",\"active\":true,\"discoveredAt\":\"2018-11-28T18:01:26.000\",\"modifiedAt\":\"2018-11-28T18:01:26.000\",\"dataStore\":\"EmptyDatastore\"},{\"id\":2,\"name\":\"MyDataStore\",\"description\":\"My DataStore\",\"metadataPath\":\"MyDataStore:\",\"type\":\"SQL\",\"subtype\":\"DS\",\"tenant\":\"stratio\",\"active\":true,\"discoveredAt\":\"2018-11-28T18:01:26.000\",\"modifiedAt\":\"2018-11-28T18:01:26.000\",\"dataStore\":\"MyDataStore\"},{\"id\":3,\"name\":\"FinantialDB\",\"description\":\"Finantial DataBase\",\"metadataPath\":\"MyDataStore://>FinantialDB/:\",\"type\":\"SQL\",\"subtype\":\"PATH\",\"tenant\":\"stratio\",\"active\":true,\"discoveredAt\":\"2018-11-28T18:01:26.000\",\"modifiedAt\":\"2018-11-28T18:01:26.000\",\"dataStore\":\"MyDataStore\"},{\"id\":4,\"name\":\"toys-department\",\"description\":\"Toys Department\",\"metadataPath\":\"MyDataStore://>FinantialDB/:toys-department\",\"type\":\"SQL\",\"subtype\":\"RESOURCE\",\"tenant\":\"stratio\",\"active\":true,\"discoveredAt\":\"2018-11-28T18:01:26.000\",\"modifiedAt\":\"2018-11-28T18:01:26.000\",\"dataStore\":\"MyDataStore\"},{\"id\":5,\"name\":\"purchaserName\",\"description\":\"Purchaser Name\",\"metadataPath\":\"MyDataStore://>FinantialDB/:toys-department:purchaserName:\",\"type\":\"SQL\",\"subtype\":\"FIELD\",\"tenant\":\"stratio\",\"active\":true,\"discoveredAt\":\"2018-11-28T18:01:26.000\",\"modifiedAt\":\"2018-11-28T18:01:26.000\",\"dataStore\":\"MyDataStore\"},{\"id\":6,\"name\":\"purchases\",\"description\":\"Purchases\",\"metadataPath\":\"MyDataStore://>FinantialDB/:toys-department:purchases:\",\"type\":\"SQL\",\"subtype\":\"FIELD\",\"tenant\":\"stratio\",\"active\":true,\"discoveredAt\":\"2018-11-28T18:01:26.000\",\"modifiedAt\":\"2018-11-28T18:01:26.000\",\"dataStore\":\"MyDataStore\"}]"
    val chunk: Array[DataAssetES] = Array(
      new DataAssetES(1,Option("EmptyStore"),Option("Empty DataStore"),"EmptyDatastore:","HDFS","DS","stratio",true, new Timestamp(milis), new Timestamp(milis)),
      new DataAssetES(2,Option("MyDataStore"),Option("My DataStore"),"MyDataStore:","SQL","DS","stratio", true, new Timestamp(milis), new Timestamp(milis)),
      new DataAssetES(3,Option("FinantialDB"),Option("Finantial DataBase"),"MyDataStore://>FinantialDB/:","SQL","PATH","stratio",true, new Timestamp(milis), new Timestamp(milis)),
      new DataAssetES(4,Option("toys-department"),Option("Toys Department"),"MyDataStore://>FinantialDB/:toys-department","SQL","RESOURCE","stratio",true, new Timestamp(milis), new Timestamp(milis)),
      new DataAssetES(5,Option("purchaserName"),Option("Purchaser Name"),"MyDataStore://>FinantialDB/:toys-department:purchaserName:","SQL","FIELD","stratio",true, new Timestamp(milis), new Timestamp(milis)),
      new DataAssetES(6,Option("purchases"),Option("Purchases"),"MyDataStore://>FinantialDB/:toys-department:purchases:","SQL","FIELD","stratio",true, new Timestamp(milis), new Timestamp(milis))
    )

    val result = process(chunk, noAdds = true)

    assertResult(reference)(result)

  }

  def process(chunk: Array[DataAssetES], noAdds: Boolean): String = {
    val s: Semaphore = new Semaphore(1)
    //
    val sourceDao: CustomTestSourceDao = new CustomTestSourceDao(noAdds)
    val eParams: ExtractorTestParams = new ExtractorTestParams(s, sourceDao, chunk)
    val piParams: PartialIndexerTestParams = new PartialIndexerTestParams(s, noAdds)

    eParams.getSemaphore.acquire()

    val actorSystem: SearcherActorSystem[SASTExtractor, DGIndexer] = new SearcherActorSystem[SASTExtractor, DGIndexer]("test", classOf[SASTExtractor], classOf[DGIndexer], eParams, piParams)
    actorSystem.initPartialIndexation()

    eParams.getSemaphore.acquire()
    eParams.getSemaphore.release()

    actorSystem.stopAll()
    piParams.getResult
  }
}