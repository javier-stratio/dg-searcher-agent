package com.stratio.governance.agent.searcher.actors.manager

import akka.actor.{Actor, ActorRef}
import akka.pattern.ask
import akka.util.Timeout
import java.util.UUID.randomUUID

import com.stratio.governance.agent.searcher.actors.manager.dao.SearcherDao
import com.stratio.governance.agent.searcher.actors.manager.utils.ManagerUtils
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.{Failure, Success}


class DGManager(extractor: ActorRef, managerUtils: ManagerUtils, searcherDao: SearcherDao) extends Actor {

  private lazy val LOG: Logger = LoggerFactory.getLogger(getClass.getName)
  implicit val timeout: Timeout = Timeout(60000, MILLISECONDS)

  val BOOT: String = "boot"
  val FIRST_TOTAL_INDEXATION: String = "first_total_indexation"
  val TOTAL_INDEXATION: String = "total_indexation"
  val PARTIAL_INDEXATION: String = "partial_indexation"

  context.system.scheduler.scheduleOnce(1000 millis, self, BOOT)

  var active_partial_indexations: Option[String]= None
  var totalIndexationPending: Boolean = false

  override def receive: Receive = {
    case BOOT => {
      LOG.info("Initiating dg-searcher-agent ...")
      val modelList: List[String] = searcherDao.getModels().filter(m => m.equals(DGManager.MODEL_NAME))

      if (modelList.isEmpty) {
        self ! FIRST_TOTAL_INDEXATION
      } else {
        launchTemporizations()
      }

    }

    case PARTIAL_INDEXATION => {
      LOG.debug("PARTIAL_INDEXATION INIT event received")
      val check: (Boolean, Option[String]) = searcherDao.checkTotalIndexation(DGManager.MODEL_NAME)
      if (!partialIndexationInProgress() && !check._1 ) {
        val token: String = getRandomToken()
        launchPartialIndexation(token)
      } else {
        LOG.warn("partial indexation can not be executed because there is another indexation (total or partial) on course")
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
      LOG.debug("TOTAL_INDEXATION INIT event received")
      if (partialIndexationInProgress()) {
        totalIndexationPending = true
      } else {
        val check: (Boolean, Option[String]) = searcherDao.checkTotalIndexation(DGManager.MODEL_NAME)
        if (!check._1) {

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
        } else {
          LOG.warn("total indexation can not be executed because there is another one on course (" + check._2.get + ")")
        }
      }
    }

    case DGManager.ManagerIndexationEvent(typ,token,status) => {
      typ match {
        case IndexationType.PARTIAL => {
          LOG.debug("PARTIAL_INDEXATION END event received")
          active_partial_indexations = None
          if (totalIndexationPending) {
            totalIndexationPending = false
            self ! TOTAL_INDEXATION
          }
        }
        case IndexationType.TOTAL => {
          LOG.debug("TOTAL_INDEXATION END event received")
          status match {
            case IndexationStatus.SUCCESS => {
              searcherDao.finishTotalIndexationProcess(DGManager.MODEL_NAME, token)
            }
            case IndexationStatus.ERROR => {
              searcherDao.cancelTotalIndexationProcess(DGManager.MODEL_NAME, token)
            }
          }
        }
      }
    }

  }

  private def partialIndexationInProgress(): Boolean = {
    val res = active_partial_indexations.isDefined
    LOG.debug("partial indexation in progress: " + res)
    res
  }

  private def getRandomToken(): String = {
    randomUUID().toString
  }

  private def launchTotalIndexation(token: String): Unit = {
      // TODO at integration use the rigth objetc
      (extractor ? "testing_total_indexation").onComplete {
        case Success(_) => {
          // Nothing to do. Wait for response
        }
        case Failure(e) => {
          LOG.warn("Error while launching total indexation", e)
          searcherDao.finishTotalIndexationProcess(DGManager.MODEL_NAME,token)
        }
      }
  }

  private def launchPartialIndexation(token: String): Unit = {
    // TODO at integration use the rigth objetc
    (extractor ? "testing_partial_indexation").onComplete {
      case Success(_) => {
        active_partial_indexations = Some(token)
      }
      case Failure(e) => {
        LOG.warn("Error while launching partial indexation", e)
        active_partial_indexations = None
      }
    }
  }

  private def launchTemporizations(): Unit = {
    // Timer initialization for both total and partial indexation
    managerUtils.getScheduler().schedulePartialIndexation(self, PARTIAL_INDEXATION)
    managerUtils.getScheduler().scheduleTotalIndexation(self,TOTAL_INDEXATION)
  }

}

object IndexationStatus extends Enumeration {
  val SUCCESS, ERROR = Value
}

object IndexationType extends Enumeration {
  val PARTIAL, TOTAL = Value
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
  case class ManagerIndexationEvent(typ: IndexationType.Value, token: String, status: IndexationStatus.Value)

}
