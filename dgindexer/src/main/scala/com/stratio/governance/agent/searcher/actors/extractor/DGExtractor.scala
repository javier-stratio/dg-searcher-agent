package com.stratio.governance.agent.searcher.actors.extractor

import akka.actor.{Actor, ActorRef}
import akka.pattern.ask
import akka.util.Timeout
import com.stratio.governance.agent.searcher.actors.dao.postgres.PostgresPartialIndexationReadState
import com.stratio.governance.agent.searcher.actors.extractor.DGExtractor.{Message, _}
import com.stratio.governance.agent.searcher.actors.indexer.DGIndexer
import com.stratio.governance.agent.searcher.actors.manager.{DGManager, IndexationStatus}
import com.stratio.governance.agent.searcher.model.es.DataAssetES
import com.stratio.governance.agent.searcher.model.utils.ExponentialBackOff
import org.json4s.DefaultFormats
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object DGExtractor {

  abstract class Message {
  }

  case class TotalIndexationMessageInit(token: String) extends Message
  case class TotalIndexationMessageEnd(token: String) extends Message
  case class TotalIndexationMessage(offset: Int, limit: Int, exponentialBackOff: ExponentialBackOff, token: String) extends Message
  case class SendTotalBatchToIndexerMessage(t: (Array[DataAssetES], Int), continue: Option[Message], exponentialBackOff: ExponentialBackOff, token: String)

  case class PartialIndexationMessageInit() extends Message
  case class PartialIndexationMessageEnd() extends Message
  case class PartialIndexationMessage(state: Option[PostgresPartialIndexationReadState], limit : Int, exponentialBackOff: ExponentialBackOff) extends Message
  case class SplitterPartialIndexationMessage(list: List[Int], limit : Int, state: Option[PostgresPartialIndexationReadState], exponentialBackOff: ExponentialBackOff) extends Message
  case class SendPartialBatchToIndexerMessage(dataAssets: Array[DataAssetES], state: Option[PostgresPartialIndexationReadState], continue: Option[Message], exponentialBackOff: ExponentialBackOff)
}

class DGExtractor(indexer: ActorRef, params: DGExtractorParams) extends Actor {

  private lazy val LOG: Logger = LoggerFactory.getLogger(getClass.getName)
  implicit val timeout: Timeout = Timeout(60, SECONDS)
  implicit val formats: DefaultFormats.type = DefaultFormats

  override def preStart(): Unit = {
    context.watch(self)
  }

  override def postStop(): Unit = {
    params.sourceDao.close()
  }

  def receive: PartialFunction[Any, Unit] = {

    case TotalIndexationMessageInit(token) =>
      LOG.debug("Initiating Total Indexation, token: " + token)
      self ! TotalIndexationMessage(0, params.limit, params.exponentialBackOff, token)

    case TotalIndexationMessage(offset, limit, exponentialBackOff: ExponentialBackOff, token) =>
      LOG.debug("Handling total Indexation messages. limit: " + limit + ", token: " + token)
      val results: (Array[DataAssetES], Int) = params.sourceDao.readDataAssetsSince(offset, limit)
      if (results._1.length == limit) {
        self ! SendTotalBatchToIndexerMessage(results, Some(DGExtractor.TotalIndexationMessage(results._2, limit, exponentialBackOff, token)), exponentialBackOff, token)
      } else {
        self ! SendTotalBatchToIndexerMessage(results, Some(DGExtractor.TotalIndexationMessageEnd(token)), exponentialBackOff, token)
      }

    case SendTotalBatchToIndexerMessage(tuple: (Array[DataAssetES], Int), continue: Option[Message], exponentialBackOff: ExponentialBackOff, token: String) =>
      LOG.debug("Sending total Indexation messages. token: " + token)
      val future = indexer ? DGIndexer.IndexerEvent(tuple._1, Some(token))
      future.onComplete {
        case Success(_) =>
          continue match {
            case Some(message) => self ! message
            case None =>
          }
        case Failure(e) =>
          LOG.warn("Total indexation fail!")
          e.printStackTrace()
          Thread.sleep(exponentialBackOff.getPause)
          self ! SendTotalBatchToIndexerMessage(tuple, continue, exponentialBackOff.next, token)
      }
      Await.result(future, timeout.duration)

    case TotalIndexationMessageEnd(token) =>
      LOG.debug("Ending Total Indexation, token: " + token)
      for (res <- context.actorSelection("/user/" + params.managerName).resolveOne()) {
        val managerActor: ActorRef = res
        managerActor ! DGManager.ManagerTotalIndexationEvent(token, IndexationStatus.SUCCESS)
      }

    case PartialIndexationMessageInit() =>
      LOG.debug("Initiating Partial Indexation")
      self ! PartialIndexationMessage(None, params.limit, params.exponentialBackOff)

    case PartialIndexationMessage(state, limit, exponentialBackOff) =>
      LOG.debug("Handling partial Indexation messages. limit: " + limit)
      var status = state.getOrElse(params.sourceDao.readPartialIndexationState())
      val results:(List[Int], PostgresPartialIndexationReadState) = params.sourceDao.readUpdatedDataAssetsIdsSince(status)
      status = results._2
      self ! SplitterPartialIndexationMessage(results._1, limit, Some(status), exponentialBackOff)

    case SplitterPartialIndexationMessage(ids, limit, status,  exponentialBackOff) =>
      LOG.debug("splitter partial Indexation messages. limit: " + limit + ", status: " + status)
      val ids_tuple = ids.splitAt(limit)
      if (ids_tuple._1.length == limit) {
        self ! SendPartialBatchToIndexerMessage(params.sourceDao.readDataAssetsWhereIdsIn(ids_tuple._1), None, Some(DGExtractor.SplitterPartialIndexationMessage(ids_tuple._2, limit, status, exponentialBackOff)), exponentialBackOff)
      } else {
        self ! SendPartialBatchToIndexerMessage(params.sourceDao.readDataAssetsWhereIdsIn(ids_tuple._1), status, Some(PartialIndexationMessageEnd()), exponentialBackOff)
      }

    case SendPartialBatchToIndexerMessage(dataAssets: Array[DataAssetES], state: Option[PostgresPartialIndexationReadState], continue: Option[Message], exponentialBackOff: ExponentialBackOff) =>
      LOG.debug("Sending Partial Indexation messages")
      val future = (indexer ? DGIndexer.IndexerEvent(dataAssets, None))
      future.onComplete{
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
          LOG.warn("Partial indexation fail!")
          e.printStackTrace()
          Thread.sleep(exponentialBackOff.getPause)
          self ! SendPartialBatchToIndexerMessage(dataAssets, state, continue, exponentialBackOff.next)
      }
      Await.result(future, timeout.duration)

    case PartialIndexationMessageEnd() =>
      LOG.debug("Ending Partial Indexation")
      for (res <- context.actorSelection("/user/" + params.managerName).resolveOne()) {
        val managerActor: ActorRef = res
        managerActor ! DGManager.ManagerPartialIndexationEvent(IndexationStatus.SUCCESS)
      }

  }

  def error: Receive = {
    case msg: AnyRef => LOG.debug(s"Actor in error state no messages processed: ${msg.getClass.getCanonicalName}")
  }
}

