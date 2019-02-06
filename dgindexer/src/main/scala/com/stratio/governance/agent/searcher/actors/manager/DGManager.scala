package com.stratio.governance.agent.searcher.actors.manager

import akka.actor.{Actor, ActorRef}
import java.util.UUID.randomUUID

import com.stratio.governance.agent.searcher.actors.extractor.DGExtractor.{PartialIndexationMessageInit, TotalIndexationMessageInit}
import com.stratio.governance.agent.searcher.actors.manager.dao.{Available, Busy, Error, SearcherDao, TotalIndexationState}
import com.stratio.governance.agent.searcher.actors.manager.utils.ManagerUtils
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._


class DGManager(extractor: ActorRef, managerUtils: ManagerUtils, searcherDao: SearcherDao) extends Actor {

  private lazy val LOG: Logger = LoggerFactory.getLogger(getClass.getName)

  val BOOT: String = "boot"
  val FIRST_TOTAL_INDEXATION: String = "first_total_indexation"
  val TOTAL_INDEXATION: String = "total_indexation"
  val PARTIAL_INDEXATION: String = "partial_indexation"

  private val UNRECOVER_ERROR: String = "Governance Search domain is in an unrecoverable Error State. Total indexation can not be done!!. Contact your administrator."

  context.system.scheduler.scheduleOnce(1000 millis, self, BOOT)

  var active_partial_indexation: Boolean = false
  var totalIndexationPending: Boolean = false

  override def receive: Receive = {
    case BOOT => {
      LOG.info("Initiating dg-searcher-agent ...")
      val modelList: List[String] = searcherDao.getModels().filter(m => m.equals(DGManager.MODEL_NAME))

      if (modelList.isEmpty) {
        self ! FIRST_TOTAL_INDEXATION
      } else {
        // Let's check if any total indexation is in progress. This must be an error
        val check: (TotalIndexationState, Option[String]) = searcherDao.checkTotalIndexation(DGManager.MODEL_NAME)
        check._1 match {
          case Busy =>
            // Such case, let's cancel launch total indexation and start another one new again
            searcherDao.cancelTotalIndexationProcess (DGManager.MODEL_NAME, check._2.get)
            launchTemporizations
            self ! TOTAL_INDEXATION
          case Available =>
            launchTemporizations
          case Error =>
            LOG.error(UNRECOVER_ERROR)
            launchTemporizations
        }
      }

    }

    case PARTIAL_INDEXATION => {
      LOG.info("PARTIAL_INDEXATION INIT event received")
      if (active_partial_indexation) {
        LOG.warn("partial indexation can not be executed because there is another partial indexation")
      } else {
        val check: (TotalIndexationState, Option[String]) = searcherDao.checkTotalIndexation(DGManager.MODEL_NAME)
        check._1 match {
          case Busy =>
            LOG.warn("partial indexation can not be executed because there is a total indexation on course")
          case Available =>
            launchPartialIndexation
          case Error =>
            LOG.warn("partial indexation will be executed but there have been an error in last total indexation")
            launchPartialIndexation
        }
      }
    }

    case FIRST_TOTAL_INDEXATION => {
      LOG.debug("FIRST_TOTAL_INDEXATION INIT event received")
      try {
        val model = managerUtils.getGeneratedModel()
        searcherDao.insertModel(DGManager.MODEL_NAME, model)

        launchTemporizations()

        self ! TOTAL_INDEXATION

      } catch {
        case e: Throwable => {
          LOG.error("Error while inserting first model. Timing not initiated", e)
        }
      }

    }

    case TOTAL_INDEXATION => {
      LOG.info("TOTAL_INDEXATION INIT event received")
      if (active_partial_indexation) {
        totalIndexationPending = true
      } else {
        val check: (TotalIndexationState, Option[String]) = searcherDao.checkTotalIndexation(DGManager.MODEL_NAME)
        check._1 match {
          case Available =>
            val token: String = searcherDao.initTotalIndexationProcess(DGManager.MODEL_NAME)
            try {
              val model = managerUtils.getGeneratedModel()
              searcherDao.insertModel(DGManager.MODEL_NAME, model)
              launchTotalIndexation(token)
            } catch {
              case e: Throwable => {
                LOG.error("Error while inserting model. token " + token, e)
                searcherDao.cancelTotalIndexationProcess(DGManager.MODEL_NAME, token)
              }
            }
          case Busy =>
            LOG.warn("total indexation can not be executed because there is another one on course (" + check._2.get + ")")
          case Error =>
            LOG.error(UNRECOVER_ERROR)
        }
      }
    }

    case DGManager.ManagerPartialIndexationEvent(status) => {
      LOG.info("PARTIAL_INDEXATION END event received")
      active_partial_indexation = false
      if (totalIndexationPending) {
        totalIndexationPending = false
        self ! TOTAL_INDEXATION
      }
    }

    case DGManager.ManagerTotalIndexationEvent(token, status) => {
      LOG.info("TOTAL_INDEXATION END event received")
      status match {
        case IndexationStatus.SUCCESS => {
          searcherDao.finishTotalIndexationProcess(DGManager.MODEL_NAME, token)
          LOG.debug("TOTAL_INDEXATION END. SUCCESS")
        }
        case IndexationStatus.ERROR => {
          searcherDao.cancelTotalIndexationProcess(DGManager.MODEL_NAME, token)
          LOG.debug("TOTAL_INDEXATION END. ERROR")
        }
      }
    }

  }

  private def getRandomToken(): String = {
    randomUUID().toString
  }

  private def launchTotalIndexation(token: String): Unit = {
    LOG.debug("Launching Total Indexation")
    extractor ! TotalIndexationMessageInit(token)
  }

  private def launchPartialIndexation: Unit = {
    LOG.debug("Launching Partial Indexation")
    extractor ! PartialIndexationMessageInit()
  }

  private def launchTemporizations(): Unit = {
    LOG.info("Initiating Partial/Total timing")
    // Timer initialization for both total and partial indexation
    managerUtils.getScheduler().schedulePartialIndexation(self, PARTIAL_INDEXATION)
    managerUtils.getScheduler().scheduleTotalIndexation(self,TOTAL_INDEXATION)
  }

}

object IndexationStatus extends Enumeration {
  val SUCCESS, ERROR = Value
}

object DGManager {

  val MODEL_NAME: String = "governance_search"

  /**
    * Default Actor name
    */
  lazy val NAME = "DGManager"

  /**
    * Actor messages
    */
  case class ManagerTotalIndexationEvent(token: String, status: IndexationStatus.Value)
  case class ManagerPartialIndexationEvent(status: IndexationStatus.Value)

}
