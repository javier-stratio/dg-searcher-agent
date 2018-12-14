package com.stratio.governance.agent.searcher.actors.extractor

import java.sql.{PreparedStatement, ResultSet, Timestamp}
import java.time.Instant
import java.util.concurrent.Semaphore

import akka.actor.Actor
import com.stratio.governance.agent.searcher.actors.SearcherActorSystem
import com.stratio.governance.agent.searcher.actors.dao.postgres.PostgresPartialIndexationReadState
import com.stratio.governance.agent.searcher.actors.extractor.dao.{SourceDao => ExtractorSourceDao}
import com.stratio.governance.agent.searcher.actors.indexer.dao.{SourceDao => IndexerSourceDao}
import com.stratio.governance.agent.searcher.actors.indexer.DGIndexer.IndexerEvent
import com.stratio.governance.agent.searcher.actors.indexer._
import com.stratio.governance.agent.searcher.actors.indexer.dao.SearcherDao
import com.stratio.governance.agent.searcher.model.EntityRow
import com.stratio.governance.agent.searcher.model.es.DataAssetES
import com.stratio.governance.agent.searcher.model.utils.ExponentialBackOff
import org.scalatest.FlatSpec

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class CustomSourceDao(chunk: Array[DataAssetES]) extends ExtractorSourceDao with IndexerSourceDao {
  val chunkList: List[(Timestamp, DataAssetES)] = chunk.toList.map(t => (t.modifiedAt, t)).sortBy(_._1.getTime)
  val byIdsList: List[(Int, DataAssetES)] = chunk.toList.map(t => (t.id, t)).sortBy(_._1)
  var lastState: PostgresPartialIndexationReadState = PostgresPartialIndexationReadState(this)

//  override def readDataAssetsSince(instant: Option[Instant], limit: Int): (Array[DataAssetES], Option[Instant]) = {
//
//  }
//
//  override def readLastIngestedInstant(): Option[Instant] = lastInstant
//
//  override def writeLastIngestedInstant(instant: Option[Instant]): Unit = {
//    lastInstant = instant
//  }

  override def close(): Unit = {}

  override def readDataAssetsSince(timestamp: Timestamp, limit: Int): (Array[DataAssetES], Timestamp) = ???

  override def readDataAssetsWhereIdsIn(param: List[Int]): Array[DataAssetES] = {
    byIdsList.filter { ids: (Int, DataAssetES) => param.contains(ids._1) }.map(_._2).toArray
  }

  override def readUpdatedDataAssetsIdsSince(state: PostgresPartialIndexationReadState): (List[Int], PostgresPartialIndexationReadState) = {
    val returnElems = chunkList.filter(_._1.after(state.readDataAsset))
    state.readDataAsset = returnElems.last._1
    (returnElems.map(_._2.id), state)
  }

  override def readPartialIndexationState(): PostgresPartialIndexationReadState = lastState

  override def writePartialIndexationState(state: PostgresPartialIndexationReadState): Unit = lastState = state

  override def prepareStatement(queryName: String): PreparedStatement = ???

  override def executeQuery(sql: String): ResultSet = ???

  override def executePreparedStatement(sql: PreparedStatement): ResultSet = ???

  override def keyValuePairProcess(ids: Array[Int]): List[EntityRow] = ???

  override def businessAssets(ids: Array[Int]): List[EntityRow] = ???
}


class testCustomSearcherDao extends SearcherDao() {

  override def indexPartial(model: String, doc: String): Unit = ???
  override def indexTotal(model: String, doc: String, token: String): Unit = ???

}
class CustomDGIndexerParams(sourceDao : CustomSourceDao, searcherDao: SearcherDao, val limit: Int, val semaphore: Semaphore) extends DGIndexerParams(sourceDao, searcherDao, 10) {

  var returnList: List[DataAssetES] = List()

  def setReturnList(list: List[DataAssetES]): Unit = {
    this.returnList = list
  }

}
class CustomDGIndexer(params: CustomDGIndexerParams) extends Actor {
  var counter: Int = 0
  var list: ArrayBuffer[DataAssetES]= ArrayBuffer()

  override def receive: Receive = {
    case IndexerEvent(chunk: Array[DataAssetES], _) =>
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

    val chunk: Array[DataAssetES] = Array(
      DataAssetES( 1, Some("fake_column"), Some("fake_description"), "fake_metadatapath", "fake_type", "fake_subtype",
        "fake_tenant", active = false, Timestamp.from(Instant.now()), Timestamp.valueOf("2010-01-01 01:01:01.001")),
      DataAssetES( 2, Some("fake_column"), Some("fake_description"), "fake_metadatapath", "fake_type", "fake_subtype",
        "fake_tenant", active = false, Timestamp.from(Instant.now()), Timestamp.valueOf("2010-01-02 01:01:01.001")),
      DataAssetES( 3, Some("fake_column"), Some("fake_description"), "fake_metadatapath", "fake_type", "fake_subtype",
        "fake_tenant", active = false, Timestamp.from(Instant.now()), Timestamp.valueOf("2010-01-03 01:01:01.001")),
      DataAssetES( 4, Some("fake_column"), Some("fake_description"), "fake_metadatapath", "fake_type", "fake_subtype",
        "fake_tenant", active = false, Timestamp.from(Instant.now()), Timestamp.valueOf("2010-01-04 01:01:01.001")),
      DataAssetES( 5, Some("fake_column"), Some("fake_description"), "fake_metadatapath", "fake_type", "fake_subtype",
        "fake_tenant", active = false, Timestamp.from(Instant.now()), Timestamp.valueOf("2010-01-05 01:01:01.001")),
      DataAssetES( 6, Some("fake_column"), Some("fake_description"), "fake_metadatapath", "fake_type", "fake_subtype",
        "fake_tenant", active = false, Timestamp.from(Instant.now()), Timestamp.valueOf("2010-01-06 01:01:01.001")),
      DataAssetES( 7, Some("fake_column"), Some("fake_description"), "fake_metadatapath", "fake_type", "fake_subtype",
        "fake_tenant", active = false, Timestamp.from(Instant.now()), Timestamp.valueOf("2010-01-07 01:01:01.001")),
      DataAssetES( 8, Some("fake_column"), Some("fake_description"), "fake_metadatapath", "fake_type", "fake_subtype",
        "fake_tenant", active = false, Timestamp.from(Instant.now()), Timestamp.valueOf("2010-01-08 01:01:01.001")),
      DataAssetES( 9, Some("fake_column"), Some("fake_description"), "fake_metadatapath", "fake_type", "fake_subtype",
        "fake_tenant", active = false, Timestamp.from(Instant.now()), Timestamp.valueOf("2010-01-09 01:01:01.001")),
      DataAssetES( 10, Some("fake_column"), Some("fake_description"), "fake_metadatapath", "fake_type", "fake_subtype",
        "fake_tenant", active = false, Timestamp.from(Instant.now()), Timestamp.valueOf("2010-01-10 01:01:01.001"))
    )

    assertResult(chunk)(process(chunk))
    // assert(result.equals(chunk), "result '" + result + "' is not '" + chunk + "'")

  }

  def process(chunk: Array[DataAssetES]): List[DataAssetES] = {
    val s: Semaphore = new Semaphore(1)

    val sourceDao: CustomSourceDao = new CustomSourceDao(chunk)
    val searcherDao: SearcherDao = new testCustomSearcherDao()

    val limit: Int = 2
    val periodMs : Long =1000
    val pauseMs: Long = 2
    val maxErrorRetry: Int= 5
    val delayMs: Long = 1000
    val eParams: DGExtractorParams = new DGExtractorParams(sourceDao, limit, periodMs, ExponentialBackOff(pauseMs, maxErrorRetry), delayMs,"test")
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
