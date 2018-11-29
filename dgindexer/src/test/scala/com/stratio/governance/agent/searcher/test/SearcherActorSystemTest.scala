package com.stratio.governance.agent.searcher.test

import java.util.concurrent.Semaphore

import akka.actor.{Actor, ActorRef, Cancellable}
import com.stratio.governance.agent.searcher.SearcherActorSystem
import com.stratio.governance.agent.searcher.actors.extractor.ExtractorParams
import com.stratio.governance.agent.searcher.actors.indexer.dao.{SearcherDao, SourceDao}
import com.stratio.governance.agent.searcher.actors.indexer.{IndexerParams}
import org.scalatest.FlatSpec
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

class CommonParams(s: Semaphore, reference: String) extends ExtractorParams with IndexerParams {

  var r: String = null

  def getSemaphore(): Semaphore = {
    return s
  }

  def getReference(): String = {
    return reference
  }

  def setResult(result: String): Unit = {
    r=result
  }

  def getResult(): String = {
    return r
  }

  override def getSourceDao(): SourceDao = ???
  override def getSearcherDao(): SearcherDao = ???

  override def getPartiton(): Int = ???
}

case class MetaInfo(value: String);

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
    val eParams: CommonParams = new CommonParams(s, reference)

    eParams.getSemaphore().acquire()

    val actorSystem: SearcherActorSystem[SimpleExtractor, SimpleIndexer] = new SearcherActorSystem[SimpleExtractor, SimpleIndexer]("test", classOf[SimpleExtractor], classOf[SimpleIndexer], eParams, eParams)
    actorSystem.initialize()

    eParams.getSemaphore().acquire()
    eParams.getSemaphore().release()

    assert(eParams.getResult().equals(reference), "result '" + eParams.getResult() + "' is not '" + reference + "'")

  }

}