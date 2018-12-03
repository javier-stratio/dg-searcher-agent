package com.stratio.governance.agent.searcher.actors

import java.time.Instant
import java.util.concurrent.Semaphore

import akka.actor.{Actor, ActorRef, Cancellable}
import com.stratio.governance.agent.searcher.actors.dao.SourceDao
import com.stratio.governance.agent.searcher.actors.extractor.DGExtractorParams
import com.stratio.governance.agent.searcher.actors.indexer.IndexerParams
import com.stratio.governance.agent.searcher.actors.indexer.dao.SearcherDao
import com.stratio.governance.agent.searcher.model._
import com.stratio.governance.commons.agent.domain.dao.DataAssetDao
import org.scalatest.FlatSpec
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class CustomSourceDao extends SourceDao {

  override def close(): Unit = ???

  override def keyValuePairProcess(ids: Array[Int]): List[EntityRow] = ???

  override def businessTerms(ids: Array[Int]): List[EntityRow] = ???

  override def readDataAssetsSince(instant: Option[Instant], limit: Int): (Array[DataAssetDao], Option[Instant]) = ???

  override def readLastIngestedInstant(): Option[Instant] = ???

  override def writeLastIngestedInstant(instant: Option[Instant]): Unit = ???
}

class CommonParams(s: Semaphore, sourceDao: SourceDao, reference: String) extends DGExtractorParams(sourceDao, 10,10, 10, 10,10) with IndexerParams {

  var r: String =""

  def getSemaphore(): Semaphore = {
    s
  }

  def getReference(): String = {
    reference
  }

  def setResult(result: String): Unit = {
    r=result
  }

  def getResult(): String = {
    r
  }

  override def getSourceDao(): SourceDao = ???
  override def getSearcherDao(): SearcherDao = ???

  override def getPartition(): Int = ???
}

case class MetaInfo(value: String)

class SimpleExtractor(indexer: ActorRef, params: CommonParams) extends Actor {

  private final val NOTIFICATION: String = "notification"

  private lazy val LOG: Logger = LoggerFactory.getLogger(getClass.getName)

  val notification_cancellable: Cancellable = context.system.scheduler.scheduleOnce(1 millis, self, NOTIFICATION)

  override def receive = {
    case NOTIFICATION => {
      println("Notification received")
      self ! MetaInfo(params.getReference())
    }
    case MetaInfo(value) => {
      println("value (Extractor): " + value)
      indexer ! MetaInfo(value)
    }
    case _       => LOG.info("Extractor default handle. Nothing to do.")
  }
}

class SimpleIndexer(params: CommonParams) extends Actor {

  private lazy val LOG: Logger = LoggerFactory.getLogger(getClass.getName)

  override def receive = {
    case MetaInfo(value) => {
      println("value (indexer): " + value)
      params.setResult(value)
      params.getSemaphore().release()
    }
    case _       => LOG.info("Indexer default handle. Nothing to do.")
  }

}

class SearcherActorSystemTest extends FlatSpec {

  "Extractor Events" should "be processed in Indexer" in {

    val s: Semaphore = new Semaphore(1)
    val reference: String = "testing"
    val sourceDao: SourceDao = new CustomSourceDao()
    val eParams: CommonParams = new CommonParams(s, sourceDao, reference)

    eParams.getSemaphore().acquire()

    val actorSystem: SearcherActorSystem[SimpleExtractor, SimpleIndexer] = new SearcherActorSystem[SimpleExtractor, SimpleIndexer]("test", classOf[SimpleExtractor], classOf[SimpleIndexer], eParams, eParams)
    actorSystem.initPartialIndexation()

    eParams.getSemaphore().acquire()
    eParams.getSemaphore().release()

    Thread.sleep(1000)
    actorSystem.stopAll()
    assert(eParams.getResult().equals(reference), "result '" + eParams.getResult() + "' is not '" + reference + "'")

  }
}