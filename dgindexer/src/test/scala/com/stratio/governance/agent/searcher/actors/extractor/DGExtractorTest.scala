package com.stratio.governance.agent.searcher.actors.extractor

import java.sql.Timestamp
import java.time.Instant
import java.util.concurrent.Semaphore

import akka.actor.Actor
import com.stratio.governance.agent.searcher.actors.SearcherActorSystem
import com.stratio.governance.agent.searcher.actors.dao.SourceDao
import com.stratio.governance.agent.searcher.actors.indexer.DGIndexer.IndexerEvent
import com.stratio.governance.agent.searcher.actors.indexer._
import com.stratio.governance.agent.searcher.actors.indexer.dao.{CustomSearcherDao, SearcherDao}
import com.stratio.governance.agent.searcher.model._
import com.stratio.governance.commons.agent.domain.dao.DataAssetDao
import org.scalatest.FlatSpec

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class CustomSourceDao(chunk: Array[DataAssetDao]) extends SourceDao {
  val chunkList: List[(Timestamp, DataAssetDao)] = chunk.toList.map(t => (t.modifiedAt, t)).sortBy(_._1.getTime)
  var lastInstant: Option[Instant] = None

  override def readDataAssetsSince(instant: Option[Instant], limit: Int): (Array[DataAssetDao], Option[Instant]) = {
    val returnElems = instant match {
      case None => {
        chunkList.take(limit)
      }
      case Some(_) => {
        val time: Timestamp = Timestamp.from(instant.get)
        chunkList.filter(_._1.after(time)).take(limit)
      }
    }
    (returnElems.map(_._2).toArray, returnElems.lastOption.map(_._1.toInstant))
  }

  override def readLastIngestedInstant(): Option[Instant] = lastInstant

  override def writeLastIngestedInstant(instant: Option[Instant]): Unit = {
    lastInstant = instant
  }

  override def close(): Unit = {}

  override def keyValuePairProcess(ids: Array[Int]): List[EntityRow] = ???

  override def businessTerms(ids: Array[Int]): List[EntityRow] = ???
}

class testCustomSearcherDao extends SearcherDao() {
  override def index(doc: String): Unit = ???
}
class CustomDGIndexerParams(sourceDao :SourceDao, searcherDao: SearcherDao, val limit: Int, val semaphore: Semaphore) extends DGIndexerParams(sourceDao, searcherDao) {

  var returnList: List[DataAssetDao] = List()

  def setReturnList(list: List[DataAssetDao]): Unit = {
    this.returnList = list
  }

}
class CustomDGIndexer(params: CustomDGIndexerParams) extends Actor {
  var counter: Int = 0
  var list: ArrayBuffer[DataAssetDao]= ArrayBuffer()

  override def receive: Receive = {
    case IndexerEvent(chunk: Array[DataAssetDao]) =>
      list ++= chunk
      if (chunk.length < params.limit) {
        params.semaphore.release()
        params.setReturnList(list.toList)
      }
      sender ! Future()
  }
}


class DGExtractorTest extends FlatSpec {

  "Extractor Extracted Simulation Events" should "be processed in Indexer Mock" in {

    val chunk: Array[DataAssetDao] = Array(
      DataAssetDao( 1, Some("fake_column"), Some("fake_description"), "fake_metadatapath", "fake_type", "fake_subtype",
        "fake_tenant", "fake_properties", active = false, Timestamp.from(Instant.now()), Timestamp.valueOf("2010-01-01 01:01:01.001")),
      DataAssetDao( 2, Some("fake_column"), Some("fake_description"), "fake_metadatapath", "fake_type", "fake_subtype",
        "fake_tenant", "fake_properties", active = false, Timestamp.from(Instant.now()), Timestamp.valueOf("2010-01-02 01:01:01.001")),
      DataAssetDao( 3, Some("fake_column"), Some("fake_description"), "fake_metadatapath", "fake_type", "fake_subtype",
        "fake_tenant", "fake_properties", active = false, Timestamp.from(Instant.now()), Timestamp.valueOf("2010-01-03 01:01:01.001")),
      DataAssetDao( 4, Some("fake_column"), Some("fake_description"), "fake_metadatapath", "fake_type", "fake_subtype",
        "fake_tenant", "fake_properties", active = false, Timestamp.from(Instant.now()), Timestamp.valueOf("2010-01-04 01:01:01.001")),
      DataAssetDao( 5, Some("fake_column"), Some("fake_description"), "fake_metadatapath", "fake_type", "fake_subtype",
        "fake_tenant", "fake_properties", active = false, Timestamp.from(Instant.now()), Timestamp.valueOf("2010-01-05 01:01:01.001")),
      DataAssetDao( 6, Some("fake_column"), Some("fake_description"), "fake_metadatapath", "fake_type", "fake_subtype",
        "fake_tenant", "fake_properties", active = false, Timestamp.from(Instant.now()), Timestamp.valueOf("2010-01-06 01:01:01.001")),
      DataAssetDao( 7, Some("fake_column"), Some("fake_description"), "fake_metadatapath", "fake_type", "fake_subtype",
        "fake_tenant", "fake_properties", active = false, Timestamp.from(Instant.now()), Timestamp.valueOf("2010-01-07 01:01:01.001")),
      DataAssetDao( 8, Some("fake_column"), Some("fake_description"), "fake_metadatapath", "fake_type", "fake_subtype",
        "fake_tenant", "fake_properties", active = false, Timestamp.from(Instant.now()), Timestamp.valueOf("2010-01-08 01:01:01.001")),
      DataAssetDao( 9, Some("fake_column"), Some("fake_description"), "fake_metadatapath", "fake_type", "fake_subtype",
        "fake_tenant", "fake_properties", active = false, Timestamp.from(Instant.now()), Timestamp.valueOf("2010-01-09 01:01:01.001")),
      DataAssetDao( 10, Some("fake_column"), Some("fake_description"), "fake_metadatapath", "fake_type", "fake_subtype",
        "fake_tenant", "fake_properties", active = false, Timestamp.from(Instant.now()), Timestamp.valueOf("2010-01-10 01:01:01.001"))
    )

    assertResult(chunk)(process(chunk))
    // assert(result.equals(chunk), "result '" + result + "' is not '" + chunk + "'")

  }

  def process(chunk: Array[DataAssetDao]): List[DataAssetDao] = {
    val s: Semaphore = new Semaphore(1)

    val sourceDao: SourceDao = new CustomSourceDao(chunk)
    val searcherDao: SearcherDao = new CustomSearcherDao()

    val limit: Int = 2
    val periodMs : Long =1000
    val pauseMs: Long = 2
    val maxErrorRetry: Int= 5
    val delayMs: Long = 1000
    val eParams: DGExtractorParams = new DGExtractorParams(sourceDao, limit, periodMs, pauseMs, maxErrorRetry, delayMs)
    val piParams: CustomDGIndexerParams = new CustomDGIndexerParams(sourceDao, searcherDao, limit, s)

    s.acquire()

    val actorSystem: SearcherActorSystem[DGExtractor, CustomDGIndexer] = new SearcherActorSystem[DGExtractor, CustomDGIndexer]("test", classOf[DGExtractor], classOf[CustomDGIndexer], eParams, piParams)
    actorSystem.initPartialIndexation()

    s.acquire()
    s.release()

    actorSystem.stopAll()
    piParams.returnList
  }
}
