package com.stratio.governance.agent.searcher.actors.indexer

import java.sql.Timestamp
import java.time.Instant
import java.util.concurrent.Semaphore

import akka.actor.{Actor, ActorRef, Cancellable}
import com.stratio.governance.agent.searcher.actors.SearcherActorSystem
import com.stratio.governance.agent.searcher.actors.dao.SourceDao
import com.stratio.governance.agent.searcher.actors.extractor.DGExtractorParams
import com.stratio.governance.agent.searcher.actors.indexer.dao.SearcherDao
import com.stratio.governance.agent.searcher.model.{BusinessTerm, EntityRow, KeyValuePair}
import com.stratio.governance.commons.agent.domain.dao.DataAssetDao
import org.scalatest.FlatSpec
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class ExtractorTestParams(s: Semaphore, sourceDao: SourceDao, chunk: Array[DataAssetDao]) extends DGExtractorParams(sourceDao, 10,10, 10, 10,10) {

  def getSemaphore(): Semaphore = {
    s
  }

  def getChunk(): Array[DataAssetDao] = {
    chunk
  }

}
class CustomTestSourceDao(noAdds: Boolean) extends SourceDao {
  override def keyValuePairProcess(ids: Array[Int]): List[EntityRow] = {
    if (!noAdds) {
      val rows: List[List[EntityRow]] = ids.map(i => List(KeyValuePair(i, "OWNER", "finantial", "2018-11-29T10:27:00.000"), KeyValuePair(i, "QUALITY", "High", "2018-09-28T20:45:00.000"))).toList
      rows.fold[List[EntityRow]](List())((a: List[EntityRow], b: List[EntityRow]) => {
        a ++ b
      }).filter(a => a.getId() != 1)
    } else {
      List[EntityRow]()
    }
  }

  override def businessTerms(ids: Array[Int]): List[EntityRow] = {
    if (!noAdds) {
      val rows: List[List[EntityRow]] = ids.map(i => List(BusinessTerm(i, "RGDP", "2018-09-28T20:45:00.000"), BusinessTerm(i, "FINANTIAL", "2018-09-28T20:45:00.000"))).toList
      rows.fold[List[EntityRow]](List())((a: List[EntityRow], b: List[EntityRow]) => {
        a ++ b
      }).filter(a => a.getId() != 1)
    } else {
      List[EntityRow]()
    }
  }

  override def close(): Unit = ???

  override def readDataAssetsSince(instant: Option[Instant], limit: Int): (Array[DataAssetDao], Option[Instant]) = ???

  override def readLastIngestedInstant(): Option[Instant] = ???

  override def writeLastIngestedInstant(instant: Option[Instant]): Unit = ???

}

class PartialIndexerTestParams(s: Semaphore, noAdds: Boolean) extends IndexerParams {

  var result: String = ""

  def getSemaphore(): Semaphore = {
    s
  }

  override def getSourceDao(): SourceDao = new CustomTestSourceDao(noAdds)

  override def getSearcherDao(): SearcherDao = new SearcherDao {
    override def index(doc: String): Unit = {
      result = doc
      s.release()
    }
  }

  def getResult(): String = {
    result
  }

  override def getPartition(): Int = {
    2
  }
}

class SASTExtractor(indexer: ActorRef, params: ExtractorTestParams) extends Actor {

  private final val NOTIFICATION: String = "notification"

  private lazy val LOG: Logger = LoggerFactory.getLogger(getClass.getName)

  val notification_cancellable: Cancellable = context.system.scheduler.scheduleOnce(1 millis, self, NOTIFICATION)

  override def receive: PartialFunction[Any, Unit] = {
    case NOTIFICATION => {
      println("Notification received")
      indexer ! DGIndexer.IndexerEvent(params.getChunk())
    }

    case _ => LOG.info("Extractor default handle. Nothing to do.")
  }

}

class DGIndexerTest extends FlatSpec {

  "Extractor Completed Events Simulation" should "be processed in Indexer Mock" in {

    val milis: Long = 1543424486000l
    val reference: String = "[{\"id\":2,\"name\":\"MyDataStore\",\"description\":\"My DataStore\",\"metadataPath\":\"MyDataStore:\",\"type\":\"SQL\",\"tenant\":\"DS\",\"active\":true,\"discoveredAt\":\"2018-11-28T18:01:26.000\",\"modifiedAt\":\"2018-11-29T10:27:00.000\",\"businessTerms\":[\"FINANTIAL\",\"RGDP\"],\"keys\":[\"QUALITY\",\"OWNER\"],\"key.QUALITY\":\"High\",\"key.OWNER\":\"finantial\"},{\"id\":1,\"name\":\"EmptyStore\",\"description\":\"Empty DataStore\",\"metadataPath\":\"EmptyDatastore:\",\"type\":\"HDFS\",\"tenant\":\"DS\",\"active\":true,\"discoveredAt\":\"2018-11-28T18:01:26.000\",\"modifiedAt\":\"2018-11-28T18:01:26.000\"},{\"id\":3,\"name\":\"FinantialDB\",\"description\":\"Finantial DataBase\",\"metadataPath\":\"MyDataStore://>FinantialDB/:\",\"type\":\"SQL\",\"tenant\":\"PATH\",\"active\":true,\"discoveredAt\":\"2018-11-28T18:01:26.000\",\"modifiedAt\":\"2018-11-29T10:27:00.000\",\"businessTerms\":[\"FINANTIAL\",\"RGDP\"],\"keys\":[\"QUALITY\",\"OWNER\"],\"key.QUALITY\":\"High\",\"key.OWNER\":\"finantial\"},{\"id\":4,\"name\":\"toys-department\",\"description\":\"Toys Department\",\"metadataPath\":\"MyDataStore://>FinantialDB/:toys-department\",\"type\":\"SQL\",\"tenant\":\"RESOURCE\",\"active\":true,\"discoveredAt\":\"2018-11-28T18:01:26.000\",\"modifiedAt\":\"2018-11-29T10:27:00.000\",\"businessTerms\":[\"FINANTIAL\",\"RGDP\"],\"keys\":[\"QUALITY\",\"OWNER\"],\"key.QUALITY\":\"High\",\"key.OWNER\":\"finantial\"},{\"id\":5,\"name\":\"purchaserName\",\"description\":\"Purchaser Name\",\"metadataPath\":\"MyDataStore://>FinantialDB/:toys-department:purchaserName:\",\"type\":\"SQL\",\"tenant\":\"FIELD\",\"active\":true,\"discoveredAt\":\"2018-11-28T18:01:26.000\",\"modifiedAt\":\"2018-11-29T10:27:00.000\",\"businessTerms\":[\"FINANTIAL\",\"RGDP\"],\"keys\":[\"QUALITY\",\"OWNER\"],\"key.QUALITY\":\"High\",\"key.OWNER\":\"finantial\"},{\"id\":6,\"name\":\"purchases\",\"description\":\"Purchases\",\"metadataPath\":\"MyDataStore://>FinantialDB/:toys-department:purchases:\",\"type\":\"SQL\",\"tenant\":\"FIELD\",\"active\":true,\"discoveredAt\":\"2018-11-28T18:01:26.000\",\"modifiedAt\":\"2018-11-29T10:27:00.000\",\"businessTerms\":[\"FINANTIAL\",\"RGDP\"],\"keys\":[\"QUALITY\",\"OWNER\"],\"key.QUALITY\":\"High\",\"key.OWNER\":\"finantial\"}]"
    val chunk: Array[DataAssetDao] = Array(
      new DataAssetDao(1,Option("EmptyStore"),Option("Empty DataStore"),"EmptyDatastore:","HDFS","DS","stratio","",true, new Timestamp(milis), new Timestamp(milis)),
      new DataAssetDao(2,Option("MyDataStore"),Option("My DataStore"),"MyDataStore:","SQL","DS","stratio","",true, new Timestamp(milis), new Timestamp(milis)),
      new DataAssetDao(3,Option("FinantialDB"),Option("Finantial DataBase"),"MyDataStore://>FinantialDB/:","SQL","PATH","stratio","",true, new Timestamp(milis), new Timestamp(milis)),
      new DataAssetDao(4,Option("toys-department"),Option("Toys Department"),"MyDataStore://>FinantialDB/:toys-department","SQL","RESOURCE","stratio","",true, new Timestamp(milis), new Timestamp(milis)),
      new DataAssetDao(5,Option("purchaserName"),Option("Purchaser Name"),"MyDataStore://>FinantialDB/:toys-department:purchaserName:","SQL","FIELD","stratio","",true, new Timestamp(milis), new Timestamp(milis)),
      new DataAssetDao(6,Option("purchases"),Option("Purchases"),"MyDataStore://>FinantialDB/:toys-department:purchases:","SQL","FIELD","stratio","",true, new Timestamp(milis), new Timestamp(milis))
    )

    val result = process(chunk, noAdds = false)

    assert(result.equals(reference), "result '" + result + "' is not '" + reference + "'")

  }

  "Extractor No Additionals Events Simulation" should "be processed in Indexer Mock" in {

    val milis: Long = 1543424486000l
    val reference: String = "[{\"id\":1,\"name\":\"EmptyStore\",\"description\":\"Empty DataStore\",\"metadataPath\":\"EmptyDatastore:\",\"type\":\"HDFS\",\"tenant\":\"DS\",\"active\":true,\"discoveredAt\":\"2018-11-28T18:01:26.000\",\"modifiedAt\":\"2018-11-28T18:01:26.000\"},{\"id\":2,\"name\":\"MyDataStore\",\"description\":\"My DataStore\",\"metadataPath\":\"MyDataStore:\",\"type\":\"SQL\",\"tenant\":\"DS\",\"active\":true,\"discoveredAt\":\"2018-11-28T18:01:26.000\",\"modifiedAt\":\"2018-11-28T18:01:26.000\"},{\"id\":3,\"name\":\"FinantialDB\",\"description\":\"Finantial DataBase\",\"metadataPath\":\"MyDataStore://>FinantialDB/:\",\"type\":\"SQL\",\"tenant\":\"PATH\",\"active\":true,\"discoveredAt\":\"2018-11-28T18:01:26.000\",\"modifiedAt\":\"2018-11-28T18:01:26.000\"},{\"id\":4,\"name\":\"toys-department\",\"description\":\"Toys Department\",\"metadataPath\":\"MyDataStore://>FinantialDB/:toys-department\",\"type\":\"SQL\",\"tenant\":\"RESOURCE\",\"active\":true,\"discoveredAt\":\"2018-11-28T18:01:26.000\",\"modifiedAt\":\"2018-11-28T18:01:26.000\"},{\"id\":5,\"name\":\"purchaserName\",\"description\":\"Purchaser Name\",\"metadataPath\":\"MyDataStore://>FinantialDB/:toys-department:purchaserName:\",\"type\":\"SQL\",\"tenant\":\"FIELD\",\"active\":true,\"discoveredAt\":\"2018-11-28T18:01:26.000\",\"modifiedAt\":\"2018-11-28T18:01:26.000\"},{\"id\":6,\"name\":\"purchases\",\"description\":\"Purchases\",\"metadataPath\":\"MyDataStore://>FinantialDB/:toys-department:purchases:\",\"type\":\"SQL\",\"tenant\":\"FIELD\",\"active\":true,\"discoveredAt\":\"2018-11-28T18:01:26.000\",\"modifiedAt\":\"2018-11-28T18:01:26.000\"}]"
    val chunk: Array[DataAssetDao] = Array(
      new DataAssetDao(1,Option("EmptyStore"),Option("Empty DataStore"),"EmptyDatastore:","HDFS","DS","stratio","",true, new Timestamp(milis), new Timestamp(milis)),
      new DataAssetDao(2,Option("MyDataStore"),Option("My DataStore"),"MyDataStore:","SQL","DS","stratio","",true, new Timestamp(milis), new Timestamp(milis)),
      new DataAssetDao(3,Option("FinantialDB"),Option("Finantial DataBase"),"MyDataStore://>FinantialDB/:","SQL","PATH","stratio","",true, new Timestamp(milis), new Timestamp(milis)),
      new DataAssetDao(4,Option("toys-department"),Option("Toys Department"),"MyDataStore://>FinantialDB/:toys-department","SQL","RESOURCE","stratio","",true, new Timestamp(milis), new Timestamp(milis)),
      new DataAssetDao(5,Option("purchaserName"),Option("Purchaser Name"),"MyDataStore://>FinantialDB/:toys-department:purchaserName:","SQL","FIELD","stratio","",true, new Timestamp(milis), new Timestamp(milis)),
      new DataAssetDao(6,Option("purchases"),Option("Purchases"),"MyDataStore://>FinantialDB/:toys-department:purchases:","SQL","FIELD","stratio","",true, new Timestamp(milis), new Timestamp(milis))
    )

    val result = process(chunk, noAdds = true)

    assert(result.equals(reference), "result '" + result + "' is not '" + reference + "'")

  }

  def process(chunk: Array[DataAssetDao], noAdds: Boolean): String = {
    val s: Semaphore = new Semaphore(1)
    //
    val sourceDao: SourceDao = new CustomTestSourceDao(noAdds)
    val eParams: ExtractorTestParams = new ExtractorTestParams(s, sourceDao, chunk)
    val piParams: PartialIndexerTestParams = new PartialIndexerTestParams(s, noAdds)

    eParams.getSemaphore().acquire()

    val actorSystem: SearcherActorSystem[SASTExtractor, DGIndexer] = new SearcherActorSystem[SASTExtractor, DGIndexer]("test", classOf[SASTExtractor], classOf[DGIndexer], eParams, piParams)
    actorSystem.initPartialIndexation()

    eParams.getSemaphore().acquire()
    eParams.getSemaphore().release()

    actorSystem.stopAll()
    piParams.getResult()
  }
}