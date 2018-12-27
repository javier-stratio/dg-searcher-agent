package com.stratio.governance.agent.searcher.actors

import java.sql.{PreparedStatement, ResultSet, Timestamp}
import java.util.concurrent.Semaphore

import akka.actor.{Actor, ActorRef, Cancellable}
import com.stratio.governance.agent.searcher.actors.dao.postgres.PostgresPartialIndexationReadState
import com.stratio.governance.agent.searcher.actors.extractor.DGExtractorParams
import com.stratio.governance.agent.searcher.actors.extractor.dao.{SourceDao => ExtractorSourceDao}
import com.stratio.governance.agent.searcher.actors.indexer.dao.{SourceDao => IndexerSourceDao}
import com.stratio.governance.agent.searcher.actors.indexer.IndexerParams
import com.stratio.governance.agent.searcher.actors.indexer.dao.SearcherDao
import com.stratio.governance.agent.searcher.actors.utils.AdditionalBusiness
import com.stratio.governance.agent.searcher.model._
import com.stratio.governance.agent.searcher.model.es.DataAssetES
import com.stratio.governance.agent.searcher.model.utils.ExponentialBackOff
import org.scalatest.FlatSpec
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class CustomSourceDao extends ExtractorSourceDao with IndexerSourceDao {
  override def close(): Unit = ???

  override def keyValuePairProcess(ids: Array[Int]): List[KeyValuePair] = ???

  override def businessAssets(ids: Array[Int]): List[BusinessAsset] = ???

  override def readDataAssetsSince(offset: Int, limit: Int): (Array[DataAssetES], Int) = ???

  override def readDataAssetsWhereIdsIn(ids: List[Int]): Array[DataAssetES] = ???

  override def readUpdatedDataAssetsIdsSince(state: PostgresPartialIndexationReadState): (List[Int], List[Int], PostgresPartialIndexationReadState) = ???

  override def readPartialIndexationState(): PostgresPartialIndexationReadState = ???

  override def writePartialIndexationState(state: PostgresPartialIndexationReadState): Unit = ???

  override def prepareStatement(queryName: String): PreparedStatement = ???

  override def executeQuery(sql: String): ResultSet = ???

  override def execute(sql: String): Unit = ???

  override def executePreparedStatement(sql: PreparedStatement): Unit = ???

  override def executeQueryPreparedStatement(sql: PreparedStatement): ResultSet = ???

  override def readBusinessTermsWhereIdsIn(ids: List[Int]): Array[DataAssetES] = ???
}

class CommonParams(s: Semaphore, sourceDao: CustomSourceDao, reference: String) extends DGExtractorParams(sourceDao, 10,10, ExponentialBackOff(10, 10),10, "test") with IndexerParams {

  var r: String =""

  def getSemaphore: Semaphore = {
    s
  }

  def getReference: String = {
    reference
  }

  def setResult(result: String): Unit = {
    r=result
  }

  def getResult: String = {
    r
  }

  override def getSourceDao: CustomSourceDao = ???
  override def getSearcherDao: SearcherDao = ???

  override def getPartition: Int = ???

  override def getAdditionalBusiness: AdditionalBusiness = new AdditionalBusiness("","bt/", "GLOSSARY", "BUSSINESS_TERMS")
}

case class MetaInfo(value: String)

class SimpleExtractor(indexer: ActorRef, params: CommonParams) extends Actor {

  private final val NOTIFICATION: String = "notification"

  private lazy val LOG: Logger = LoggerFactory.getLogger(getClass.getName)

  val notification_cancellable: Cancellable = context.system.scheduler.scheduleOnce(1 millis, self, NOTIFICATION)

  override def receive: PartialFunction[Any, Unit] = {
    case NOTIFICATION =>
      println("Notification received")
      self ! MetaInfo(params.getReference)
    case MetaInfo(value) =>
      println("value (Extractor): " + value)
      indexer ! MetaInfo(value)
    case _       => LOG.info("Extractor default handle. Nothing to do.")
  }
}

class SimpleIndexer(params: CommonParams) extends Actor {

  private lazy val LOG: Logger = LoggerFactory.getLogger(getClass.getName)

  override def receive: PartialFunction[Any, Unit] = {
    case MetaInfo(value) =>
      println("value (indexer): " + value)
      params.setResult(value)
      params.getSemaphore.release()
    case _       => LOG.info("Indexer default handle. Nothing to do.")
  }

}

class SearcherActorSystemTest extends FlatSpec {

  "Extractor Events" should "be processed in Indexer" in {

    val s: Semaphore = new Semaphore(1)
    val reference: String = "testing"
    val sourceDao: CustomSourceDao = new CustomSourceDao()
    val eParams: CommonParams = new CommonParams(s, sourceDao, reference)

    eParams.getSemaphore.acquire()

    val actorSystem: SearcherActorSystem[SimpleExtractor, SimpleIndexer] = new SearcherActorSystem[SimpleExtractor, SimpleIndexer]("test", classOf[SimpleExtractor], classOf[SimpleIndexer], eParams, eParams)
    actorSystem.initPartialIndexation()

    eParams.getSemaphore.acquire()
    eParams.getSemaphore.release()

    Thread.sleep(1000)
    actorSystem.stopAll()
    assert(eParams.getResult.equals(reference), "result '" + eParams.getResult + "' is not '" + reference + "'")

  }
}