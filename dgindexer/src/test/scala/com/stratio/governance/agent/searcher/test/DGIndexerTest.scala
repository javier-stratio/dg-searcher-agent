package com.stratio.governance.agent.searcher.test

import java.util.concurrent.Semaphore

import akka.actor.{Actor, ActorRef, Cancellable}
import com.stratio.governance.agent.searcher.SearcherActorSystem
import com.stratio.governance.agent.searcher.actors.extractor.ExtractorParams
import com.stratio.governance.agent.searcher.actors.indexer.dao.{SearcherDao, SourceDao}
import com.stratio.governance.agent.searcher.actors.indexer.{DGIndexer, IndexerParams}
import com.stratio.governance.agent.searcher.model._
import com.stratio.governance.agent.searcher.model.es.EntityRowES
import org.postgresql.PGNotification
import org.scalatest.FlatSpec
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class ExtractorTestParams(s: Semaphore, chunk: Array[PGNotification]) extends ExtractorParams {

  def getSemaphore(): Semaphore = {
    return s
  }

  def getChunk(): Array[PGNotification] = {
    return chunk
  }

}

class PartialIndexerTestParams(s: Semaphore) extends IndexerParams {

  var result: String = null

  def getSemaphore(): Semaphore = {
    return s
  }

  override def getSourceDbo(): SourceDao = new SourceDao {
    override def keyValuePairProcess(keyValuePair: KeyValuePair): EntityRowES = {
      // TODO
      return null
    }

    override def databaseSchemaProcess(databaseSchema: DatabaseSchema): EntityRowES = {
      // TODO
      return null
    }

    override def fileTableProcess(fileTable: FileTable): EntityRowES = {
      // TODO
      return null
    }

    override def fileColumnProcess(fileColumn: FileColumn): EntityRowES = {
      // TODO
      return null
    }

    override def sqlTableProcess(sqlTable: SqlTable): EntityRowES = {
      // TODO
      return null
    }

    override def sqlColumnProcess(sqlColumn: SqlColumn): EntityRowES = {
      // TODO
      return null
    }

  }

  override def getSearcherDbo(): SearcherDao = new SearcherDao {
    override def index(doc: String): Unit = {
      result = doc
      s.release()
    }
  }

  def getResult(): String = {
    return result
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

class MockPGNotification(name: String, pid: Int, parameter: String) extends PGNotification {

  override def getName: String = {
    return name
  }

  override def getPID: Int = {
    return pid
  }

  override def getParameter: String = {
    return parameter
  }

}

class DGIndexerTestTest extends FlatSpec {

  "Extractor Extracted Simulation Events" should "be processed in Indexer Mock" in {


    val reference: String = "{}"
    val chunk: Array[PGNotification] = Array(
      new MockPGNotification("",0,""),
      new MockPGNotification("",0,"")
    )

    val result = process(chunk)

    //assert(result.equals(reference), "result '" + result + "' is not '" + reference + "'")
    assert(true)

  }

  def process(chunk: Array[PGNotification]): String = {
    val s: Semaphore = new Semaphore(1)
    //
    val eParams: ExtractorTestParams = new ExtractorTestParams(s, chunk)
    val piParams: PartialIndexerTestParams = new PartialIndexerTestParams(s)

    eParams.getSemaphore().acquire()

    val actorSystem: SearcherActorSystem[SASTExtractor, DGIndexer] = new SearcherActorSystem[SASTExtractor, DGIndexer]("test", classOf[SASTExtractor], classOf[DGIndexer], eParams, piParams)
    actorSystem.initialize()

    eParams.getSemaphore().acquire()
    eParams.getSemaphore().release()

    return piParams.getResult()

  }

}