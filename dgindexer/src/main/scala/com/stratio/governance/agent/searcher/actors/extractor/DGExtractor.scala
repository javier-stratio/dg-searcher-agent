package com.stratio.governance.agent.searcher.actors.extractor

import java.sql.Timestamp
import java.time.Instant

import akka.actor.{Actor, ActorRef}
import akka.pattern.ask
import akka.util.Timeout
import com.stratio.governance.agent.searcher.actors.dao.postgres.PostgresPartialIndexationReadState
import com.stratio.governance.agent.searcher.actors.extractor.DGExtractor.{Message, _}
import com.stratio.governance.agent.searcher.actors.indexer.DGIndexer
import com.stratio.governance.agent.searcher.model.es.DataAssetES
import com.stratio.governance.agent.searcher.model.utils.ExponentialBackOff
import org.json4s.DefaultFormats
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object DGExtractor {

  abstract class Message {
  }

  case class TotalIndexationMessage(timestamp: Timestamp, limit: Int, exponentialBackOff: ExponentialBackOff) extends Message
  case class SendTotalBatchToIndexerMessage(t: (Array[DataAssetES], Timestamp), continue: Option[Message], exponentialBackOff: ExponentialBackOff)

  case class PartialIndexationMessage(state: Option[PostgresPartialIndexationReadState], limit : Int, exponentialBackOff: ExponentialBackOff) extends Message
  case class SplitterPartialIndexationMessage(list: List[Int], limit : Int, state: Option[PostgresPartialIndexationReadState], exponentialBackOff: ExponentialBackOff) extends Message
  case class SendPartialBatchToIndexerMessage(dataAssets: Array[DataAssetES], state: Option[PostgresPartialIndexationReadState], continue: Option[Message], exponentialBackOff: ExponentialBackOff)
}

class DGExtractor(indexer: ActorRef, params: DGExtractorParams) extends Actor {

  private lazy val LOG: Logger = LoggerFactory.getLogger(getClass.getName)
  implicit val timeout: Timeout = Timeout(60000, MILLISECONDS)
  implicit val formats: DefaultFormats.type = DefaultFormats

  override def preStart(): Unit = {
    context.watch(self)
  }

  override def postStop(): Unit = {
    params.sourceDao.close()
  }

  def receive: PartialFunction[Any, Unit] = {

    case TotalIndexationMessage(timestamp, limit, exponentialBackOff: ExponentialBackOff) =>
      val results: (Array[DataAssetES], Timestamp) = params.sourceDao.readDataAssetsSince(timestamp, limit)
      if (results._1.length == limit) {
        self ! SendTotalBatchToIndexerMessage(results, Some(DGExtractor.TotalIndexationMessage(results._2, limit, exponentialBackOff)), exponentialBackOff)
      } else {
        self ! SendTotalBatchToIndexerMessage(results, None, exponentialBackOff)
      }

    case SendTotalBatchToIndexerMessage(tuple: (Array[DataAssetES], Timestamp), continue: Option[Message], exponentialBackOff: ExponentialBackOff) =>
      (indexer ? DGIndexer.IndexerEvent(tuple._1)).onComplete{
        case Success(_) =>
          continue match {
            case Some(message) => self ! message
            case None =>
          }
        case Failure(e) =>
          println(s"Indexation failed")
          e.printStackTrace()
          Thread.sleep(exponentialBackOff.getPause)
          self ! SendTotalBatchToIndexerMessage(tuple, continue, exponentialBackOff.next)
      }

    case PartialIndexationMessage(state, limit, exponentialBackOff) =>
      var status = state.getOrElse(params.sourceDao.readPartialIndexationState())
      val results:(List[Int], PostgresPartialIndexationReadState) = params.sourceDao.readUpdatedDataAssetsIdsSince(status)
      status = results._2
      self ! SplitterPartialIndexationMessage(results._1, limit, Some(status), exponentialBackOff)

    case SplitterPartialIndexationMessage(ids, limit, status,  exponentialBackOff) =>
      val ids_tuple = ids.splitAt(limit)
      if (ids_tuple._1.length == limit) {
        self ! SendPartialBatchToIndexerMessage(params.sourceDao.readDataAssetsWhereIdsIn(ids_tuple._1), None, Some(DGExtractor.SplitterPartialIndexationMessage(ids_tuple._2, limit, status, exponentialBackOff)), exponentialBackOff)
      } else {
        self ! SendPartialBatchToIndexerMessage(params.sourceDao.readDataAssetsWhereIdsIn(ids_tuple._1), status, None, exponentialBackOff)
      }

    case SendPartialBatchToIndexerMessage(dataAssets: Array[DataAssetES], state: Option[PostgresPartialIndexationReadState], continue: Option[Message], exponentialBackOff: ExponentialBackOff) =>
      (indexer ? DGIndexer.IndexerEvent(dataAssets)).onComplete{
        case Success(_) =>
          state match {
            case Some(s) => params.sourceDao.writePartialIndexationState(s)
            case None =>
          }
          continue match {
            case Some(_) => self ! continue.get
            case None =>
          }
        case Failure(e) =>
          println(s"Indexation failed")
          e.printStackTrace()
          Thread.sleep(exponentialBackOff.getPause)
          self ! SendPartialBatchToIndexerMessage(dataAssets, state, continue, exponentialBackOff.next)
      }
  }

  def error: Receive = {
    case msg: AnyRef => LOG.debug(s"Actor in error state no messages processed: ${msg.getClass.getCanonicalName}")
  }
}

