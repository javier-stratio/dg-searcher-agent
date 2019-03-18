package com.stratio.governance.agent.searcher.actors.extractor

import akka.actor.{Actor, ActorRef}
import akka.pattern.ask
import akka.util.Timeout
import com.stratio.governance.agent.searcher.actors.dao.postgres.PostgresPartialIndexationReadState
import com.stratio.governance.agent.searcher.actors.extractor.DGExtractor.{Message, _}
import com.stratio.governance.agent.searcher.actors.indexer.DGIndexer
import com.stratio.governance.agent.searcher.actors.manager.{DGManager, IndexationStatus}
import com.stratio.governance.agent.searcher.model.es.ElasticObject
import com.stratio.governance.agent.searcher.model.utils.ExponentialBackOff
import org.json4s.DefaultFormats
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer
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
  case class SendTotalBatchToIndexerMessage(t: (Array[ElasticObject], Int), continue: Option[Message], exponentialBackOff: ExponentialBackOff, token: String)

  case class PartialIndexationMessageInit() extends Message
  case class PartialIndexationMessageEnd() extends Message
  case class PartialIndexationMessage(state: Option[PostgresPartialIndexationReadState], limit : Int, exponentialBackOff: ExponentialBackOff) extends Message
  case class SplitterPartialIndexationMessage(list: List[String], limit : Int, state: Option[PostgresPartialIndexationReadState], exponentialBackOff: ExponentialBackOff, addList: List[SendPartialIndexationAdditionalBusiness]) extends Message
  case class SendPartialBatchToIndexerMessage(dataAssets: Array[ElasticObject], state: Option[PostgresPartialIndexationReadState], continue: Option[Message], exponentialBackOff: ExponentialBackOff, addList: List[SendPartialIndexationAdditionalBusiness]) extends Message

  case class PartialIndexationAdditionalBusinessProcess(adds: List[SendPartialIndexationAdditionalBusiness])
  abstract class SendPartialIndexationAdditionalBusiness(ids: List[Int])
  case class SendPartialIndexationBusinessTerm(ids: List[Int], state: PostgresPartialIndexationReadState, exponentialBackOff: ExponentialBackOff) extends SendPartialIndexationAdditionalBusiness(ids)
  case class SendPartialIndexationQualityRules(ids: List[Int], state: PostgresPartialIndexationReadState, exponentialBackOff: ExponentialBackOff) extends SendPartialIndexationAdditionalBusiness(ids)

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
      val results: (Array[ElasticObject], Int) = params.sourceDao.readDataAssetsSince(offset, limit)
      if (results._1.length == limit) {
        self ! SendTotalBatchToIndexerMessage(results, Some(DGExtractor.TotalIndexationMessage(results._2, limit, exponentialBackOff, token)), exponentialBackOff, token)
      } else {
        self ! SendTotalBatchToIndexerMessage(results, Some(DGExtractor.TotalIndexationMessageEnd(token)), exponentialBackOff, token)
      }

    case SendTotalBatchToIndexerMessage(tuple: (Array[ElasticObject], Int), continue: Option[Message], exponentialBackOff: ExponentialBackOff, token: String) =>
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
      val results:(List[String], List[Int], List[Int], PostgresPartialIndexationReadState) = params.sourceDao.readUpdatedDataAssetsIdsSince(status)
      status = results._4
      val additionalBusiness: ListBuffer[SendPartialIndexationAdditionalBusiness] = ListBuffer()


      if (results._2.nonEmpty) {
        additionalBusiness += SendPartialIndexationBusinessTerm(results._2, status, exponentialBackOff)
      }
      if (results._3.nonEmpty) {
        additionalBusiness += SendPartialIndexationQualityRules(results._3, status, exponentialBackOff)
      }
      self ! SplitterPartialIndexationMessage(results._1, limit, Some(status), exponentialBackOff, additionalBusiness.toList)

    case SplitterPartialIndexationMessage(mdps, limit, status,  exponentialBackOff, additionalBusiness) =>
      LOG.debug("splitter partial Indexation messages. limit: " + limit + ", status: " + status)
      val mdps_tuple = mdps.splitAt(limit)
      if (mdps_tuple._1.length == limit) {
        self ! SendPartialBatchToIndexerMessage(params.sourceDao.readDataAssetsWhereMdpsIn(mdps_tuple._1), None, Some(DGExtractor.SplitterPartialIndexationMessage(mdps_tuple._2, limit, status, exponentialBackOff, additionalBusiness)), exponentialBackOff, additionalBusiness)
      } else {
        self ! SendPartialBatchToIndexerMessage(params.sourceDao.readDataAssetsWhereMdpsIn(mdps_tuple._1), status, Some(PartialIndexationMessageEnd()), exponentialBackOff, additionalBusiness)
      }

    case SendPartialBatchToIndexerMessage(dataAssets: Array[ElasticObject], state: Option[PostgresPartialIndexationReadState], continue: Option[Message], exponentialBackOff: ExponentialBackOff, additionalBusiness) =>
      LOG.debug("Sending Partial Indexation messages")
      val future = (indexer ? DGIndexer.IndexerEvent(dataAssets, None))
      future.onComplete{
        case Success(_) =>
          state match {
            case Some(s) =>
              if (additionalBusiness.isEmpty)
                params.sourceDao.writePartialIndexationState(s)
              else
                self ! PartialIndexationAdditionalBusinessProcess(additionalBusiness)
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
          self ! SendPartialBatchToIndexerMessage(dataAssets, state, continue, exponentialBackOff.next, additionalBusiness)
      }
      Await.result(future, timeout.duration)

    case PartialIndexationMessageEnd() =>
      LOG.debug("Ending Partial Indexation")
      for (res <- context.actorSelection("/user/" + params.managerName).resolveOne()) {
        val managerActor: ActorRef = res
        managerActor ! DGManager.ManagerPartialIndexationEvent(IndexationStatus.SUCCESS)
      }

    case PartialIndexationAdditionalBusinessProcess(adds: List[SendPartialIndexationAdditionalBusiness]) =>
      LOG.debug("Partial Indexation Additional Business Process")
      val addToIndex: (Array[ElasticObject], PostgresPartialIndexationReadState, ExponentialBackOff) = adds.head match {
        case SendPartialIndexationBusinessTerm(ids, state, exponentialBackOff) =>
          (params.sourceDao.readBusinessTermsWhereIdsIn(ids), state, exponentialBackOff)
        case SendPartialIndexationQualityRules(ids, state, exponentialBackOff) =>
          (params.sourceDao.readQualityRulesWhereIdsIn(ids), state, exponentialBackOff)
      }
      val future = (indexer ? DGIndexer.IndexerEvent(addToIndex._1, None))
      future.onComplete{
        case Success(_) =>
          val list = adds.tail
          if (list.isEmpty) {
            params.sourceDao.writePartialIndexationState(addToIndex._2)
          } else {
              self ! PartialIndexationAdditionalBusinessProcess(list)
          }
        case Failure(e) =>
          LOG.warn("Partial indexation of Business Terms fail!")
          e.printStackTrace()
          Thread.sleep(addToIndex._3.getPause)
          self ! PartialIndexationAdditionalBusinessProcess(adds)
      }
      Await.result(future, timeout.duration)

  }

  def error: Receive = {
    case msg: AnyRef => LOG.debug(s"Actor in error state no messages processed: ${msg.getClass.getCanonicalName}")
  }
}

