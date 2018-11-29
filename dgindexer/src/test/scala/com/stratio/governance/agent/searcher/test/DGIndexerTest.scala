package com.stratio.governance.agent.searcher.test

import java.sql.Timestamp
import java.util.concurrent.Semaphore

import akka.actor.{Actor, ActorRef, Cancellable}
import com.stratio.governance.agent.searcher.SearcherActorSystem
import com.stratio.governance.agent.searcher.actors.extractor.ExtractorParams
import com.stratio.governance.agent.searcher.actors.indexer.dao.{SearcherDao, SourceDao}
import com.stratio.governance.agent.searcher.actors.indexer.{DGIndexer, IndexerParams}
import com.stratio.governance.agent.searcher.model.{BusinessTerm, KeyValuePair ,EntityRow}
import com.stratio.governance.commons.agent.domain.dao.DataAssetDao
import org.scalatest.FlatSpec
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class ExtractorTestParams(s: Semaphore, chunk: Array[DataAssetDao]) extends ExtractorParams {

  def getSemaphore(): Semaphore = {
    return s
  }

  def getChunk(): Array[DataAssetDao] = {
    return chunk
  }

}

class PartialIndexerTestParams(s: Semaphore, noAdds: Boolean) extends IndexerParams {

  var result: String = null

  def getSemaphore(): Semaphore = {
    return s
  }

  override def getSourceDao(): SourceDao = new SourceDao {
    override def keyValuePairProcess(ids: Array[Int]): List[EntityRow] = {
      if (!noAdds) {
        val rows: List[List[EntityRow]] = ids.map(i => List(new KeyValuePair(i, "OWNER", "finantial", "2018-11-29T10:27:00.000"), new KeyValuePair(i, "QUALITY", "High", "2018-09-28T20:45:00.000"))).toList
        rows.fold[List[EntityRow]](List())((a: List[EntityRow], b: List[EntityRow]) => {
          a ++ b
        }).filter(a => a.getId() != 1)
      } else {
        List[EntityRow]()
      }
    }

    override def businessTerms(ids: Array[Int]): List[EntityRow] = {
      if (!noAdds) {
        val rows: List[List[EntityRow]] = ids.map(i => List(new BusinessTerm(i, "RGDP", "2018-09-28T20:45:00.000"), new BusinessTerm(i, "FINANTIAL", "2018-09-28T20:45:00.000"))).toList
        rows.fold[List[EntityRow]](List())((a: List[EntityRow], b: List[EntityRow]) => {
          a ++ b
        }).filter(a => a.getId() != 1)
      } else {
        List[EntityRow]()
      }
    }
  }

  override def getSearcherDao(): SearcherDao = new SearcherDao {
    override def index(doc: String): Unit = {
      result = doc
      s.release()
    }
  }

  def getResult(): String = {
    return result
  }

  override def getPartiton(): Int = {
    return 2
  }
}

class SASTExtractor(indexer: ActorRef, params: ExtractorTestParams) extends Actor {

  private final val NOTIFICATION: String = "notification"

  private lazy val LOG: Logger = LoggerFactory.getLogger(getClass.getName)

  val notification_cancellable: Cancellable = context.system.scheduler.scheduleOnce(1 millis, self, NOTIFICATION)

  override def receive = {
    case NOTIFICATION => {
      println("Notification received")
      indexer ! DGIndexer.IndexerEvent(params.getChunk())
    }

    case _       => LOG.info("Extractor default handle. Nothing to do.")
  }

}

class DGIndexerTestTest extends FlatSpec {

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

    val result = process(chunk, false)

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

    val result = process(chunk, true)

    assert(result.equals(reference), "result '" + result + "' is not '" + reference + "'")

  }

  def process(chunk: Array[DataAssetDao], noAdds: Boolean): String = {
    val s: Semaphore = new Semaphore(1)
    //
    val eParams: ExtractorTestParams = new ExtractorTestParams(s, chunk)
    val piParams: PartialIndexerTestParams = new PartialIndexerTestParams(s, noAdds)

    eParams.getSemaphore().acquire()

    val actorSystem: SearcherActorSystem[SASTExtractor, DGIndexer] = new SearcherActorSystem[SASTExtractor, DGIndexer]("test", classOf[SASTExtractor], classOf[DGIndexer], eParams, piParams)
    actorSystem.initialize()

    eParams.getSemaphore().acquire()
    eParams.getSemaphore().release()

    return piParams.getResult()

  }

}