package com.stratio.governance.agent.searcher.actors.extractor

import java.sql.{PreparedStatement, ResultSet, Timestamp}
import java.time.Instant
import java.util.concurrent.Semaphore

import akka.actor.Actor
import com.stratio.governance.agent.searcher.actors.SearcherActorSystem
import com.stratio.governance.agent.searcher.actors.dao.postgres.PostgresPartialIndexationReadState
import com.stratio.governance.agent.searcher.actors.dao.searcher.DGSearcherDaoException
import com.stratio.governance.agent.searcher.actors.extractor.dao.{ReaderElementDao, SourceDao => ExtractorSourceDao}
import com.stratio.governance.agent.searcher.actors.indexer.dao.{SourceDao => IndexerSourceDao}
import com.stratio.governance.agent.searcher.actors.indexer.DGIndexer.IndexerEvent
import com.stratio.governance.agent.searcher.actors.indexer._
import com.stratio.governance.agent.searcher.actors.indexer.dao.SearcherDao
import com.stratio.governance.agent.searcher.main.AppConf
import com.stratio.governance.agent.searcher.model.EntityRow
import com.stratio.governance.agent.searcher.model.es.ElasticObject
import com.stratio.governance.agent.searcher.model.utils.ExponentialBackOff
import org.scalatest.FlatSpec

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class CustomSourceDao(chunk: Array[ElasticObject]) extends ExtractorSourceDao with ReaderElementDao with IndexerSourceDao {
  val chunkList: List[(Timestamp, ElasticObject)] = chunk.toList.map(t => (t.modifiedAt, t)).sortBy(_._1.getTime)
  val byIdsList: List[(String, ElasticObject)] = chunk.toList.map(t => (t.metadataPath, t)).sortBy(_._1)
  var lastState: PostgresPartialIndexationReadState = PostgresPartialIndexationReadState(this, AppConf.sourceSchema)

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

  override def readElementsSince(offset: Int, limit: Int): (Array[ElasticObject], Int) = ???

  override def readDataAssetsWhereMdpsIn(param: List[String]): Array[ElasticObject] = {
    byIdsList.filter { ids: (String, ElasticObject) => param.contains(ids._1) }.map(_._2).toArray
  }

  override def readUpdatedElementsIdsSince(state: PostgresPartialIndexationReadState): (List[String], List[Int], List[Int], PostgresPartialIndexationReadState) = {
    val returnElems = chunkList.filter(_._1.after(state.fields.get("last_read_data_asset").get._2))
    state.fields = state.fields.map(e => {
      if (e._1 == "last_read_data_asset") {
        (e._1, (e._2._1, returnElems.last._1))
      } else {
        e
      }
    })
    (returnElems.map(_._2.metadataPath), List(), List(), state)
  }

  override def readPartialIndexationState(): PostgresPartialIndexationReadState = lastState

  override def writePartialIndexationState(state: PostgresPartialIndexationReadState): Unit = lastState = state

  override def prepareStatement(queryName: String): PreparedStatement = ???

  override def executeQuery(sql: String): ResultSet = ???

  override def keyValuePairForDataAsset(mdps: List[String]): List[EntityRow] = ???

  override def businessTermsForDataAsset(mdps: List[String]): List[EntityRow] = ???

  override def execute(sql: String): Unit = ???

  override def executePreparedStatement(sql: PreparedStatement): Unit = ???

  override def executeQueryPreparedStatement(sql: PreparedStatement): ResultSet = ???

  override def readBusinessAssetsWhereIdsIn(ids: List[Int]): Array[ElasticObject] = ???

  override def qualityRulesForDataAsset(mdps: List[String]): List[EntityRow] = ???

  override def readQualityRulesWhereIdsIn(ids: List[Int]): Array[ElasticObject] = ???

  override def keyValuePairForBusinessAsset(ids: List[Long]): List[EntityRow] = ???

  override def keyValuePairForQualityRule(ids: List[Long]): List[EntityRow] = ???

  override def businessRulesForQualityRule(ids: List[Long]): List[EntityRow] = ???
}


class testCustomSearcherDao extends SearcherDao() {

  override def indexPartial(model: String, doc: String): Option[DGSearcherDaoException] = ???
  override def indexTotal(model: String, doc: String, token: String): Option[DGSearcherDaoException] = ???

}
class CustomDGIndexerParams(sourceDao : CustomSourceDao, searcherDao: SearcherDao, val limit: Int, val semaphore: Semaphore) extends DGIndexerParams(sourceDao, searcherDao, 10, null) {

  var returnList: List[ElasticObject] = List()

  def setReturnList(list: List[ElasticObject]): Unit = {
    this.returnList = list
  }

}
class CustomDGIndexer(params: CustomDGIndexerParams) extends Actor {
  var counter: Int = 0
  var list: ArrayBuffer[ElasticObject]= ArrayBuffer()

  override def receive: Receive = {
    case IndexerEvent(chunk: Array[ElasticObject], _) =>
      list ++= chunk
      if (chunk.length < params.limit) {
        params.setReturnList(list.toList)
        params.semaphore.release()
      }
      sender ! Future()
  }
}


class DGExtractorTest extends FlatSpec {

  "Extractor Extracted Simulation Events" should "be processed in Indexer Mock" in {

    val chunk: Array[ElasticObject] = Array(
      ElasticObject( "1", Some("fake_column"), None, Some("fake_description"), "fake_metadatapath_01", "fake_type", "fake_subtype",
        "fake_tenant", active = false, Timestamp.from(Instant.now()), Timestamp.valueOf("2010-01-01 01:01:01.001")),
      ElasticObject( "2", Some("fake_column"), None, Some("fake_description"), "fake_metadatapath_02", "fake_type", "fake_subtype",
        "fake_tenant", active = false, Timestamp.from(Instant.now()), Timestamp.valueOf("2010-01-02 01:01:01.001")),
      ElasticObject( "3", Some("fake_column"), None, Some("fake_description"), "fake_metadatapath_03", "fake_type", "fake_subtype",
        "fake_tenant", active = false, Timestamp.from(Instant.now()), Timestamp.valueOf("2010-01-03 01:01:01.001")),
      ElasticObject( "4", Some("fake_column"), None, Some("fake_description"), "fake_metadatapath_04", "fake_type", "fake_subtype",
        "fake_tenant", active = false, Timestamp.from(Instant.now()), Timestamp.valueOf("2010-01-04 01:01:01.001")),
      ElasticObject( "5", Some("fake_column"), None, Some("fake_description"), "fake_metadatapath_05", "fake_type", "fake_subtype",
        "fake_tenant", active = false, Timestamp.from(Instant.now()), Timestamp.valueOf("2010-01-05 01:01:01.001")),
      ElasticObject( "6", Some("fake_column"), None, Some("fake_description"), "fake_metadatapath_06", "fake_type", "fake_subtype",
        "fake_tenant", active = false, Timestamp.from(Instant.now()), Timestamp.valueOf("2010-01-06 01:01:01.001")),
      ElasticObject( "7", Some("fake_column"), None, Some("fake_description"), "fake_metadatapath_07", "fake_type", "fake_subtype",
        "fake_tenant", active = false, Timestamp.from(Instant.now()), Timestamp.valueOf("2010-01-07 01:01:01.001")),
      ElasticObject( "8", Some("fake_column"), None, Some("fake_description"), "fake_metadatapath_08", "fake_type", "fake_subtype",
        "fake_tenant", active = false, Timestamp.from(Instant.now()), Timestamp.valueOf("2010-01-08 01:01:01.001")),
      ElasticObject( "9", Some("fake_column"), None, Some("fake_description"), "fake_metadatapath_09", "fake_type", "fake_subtype",
        "fake_tenant", active = false, Timestamp.from(Instant.now()), Timestamp.valueOf("2010-01-09 01:01:01.001")),
      ElasticObject( "10", Some("fake_column"), None, Some("fake_description"), "fake_metadatapath_10", "fake_type", "fake_subtype",
        "fake_tenant", active = false, Timestamp.from(Instant.now()), Timestamp.valueOf("2010-01-10 01:01:01.001"))
    )

    assertResult(chunk)(process(chunk))
    // assert(result.equals(chunk), "result '" + result + "' is not '" + chunk + "'")

  }

  def process(chunk: Array[ElasticObject]): List[ElasticObject] = {
    val s: Semaphore = new Semaphore(1)

    val sourceDao: CustomSourceDao = new CustomSourceDao(chunk)
    val searcherDao: SearcherDao = new testCustomSearcherDao()

    val limit: Int = 2
    val periodMs : Long =1000
    val pauseMs: Long = 2
    val maxErrorRetry: Int= 5
    val delayMs: Long = 1000
    val eParams: DGExtractorParams = new DGExtractorParams(sourceDao, sourceDao, limit, periodMs, ExponentialBackOff(pauseMs, maxErrorRetry), delayMs,"test")
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
