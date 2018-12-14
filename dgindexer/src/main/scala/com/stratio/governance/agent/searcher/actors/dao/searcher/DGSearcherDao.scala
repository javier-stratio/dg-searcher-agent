package com.stratio.governance.agent.searcher.actors.dao.searcher

import com.stratio.governance.agent.searcher.http.{HttpException, HttpManager}
import org.json4s._
import org.json4s.native.JsonMethods.parse
import org.slf4j.{Logger, LoggerFactory}


class DGSearcherDao(httpManager: HttpManager) extends
  com.stratio.governance.agent.searcher.actors.indexer.dao.SearcherDao with
  com.stratio.governance.agent.searcher.actors.manager.dao.SearcherDao
{
  private lazy val LOG: Logger = LoggerFactory.getLogger(getClass.getName)
  implicit val formats: DefaultFormats.type = DefaultFormats

  val FINAL_STATUS_OK: String = "ENDED"
  val FINAL_STATUS_NOK: String = "CANCELLED"
  val INDEXING_STATUS: String = "INDEXING"

  @throws(classOf[DGSearcherDaoException])
  def indexPartial(model:  String, doc: String): Unit = {
    try {
      LOG.debug("indexing partial ...")
      val result: String = httpManager.partialPostRequest(model, doc)
      val json = parse(result)
      val resp: Indexer  = json.extract[Indexer]
      LOG.debug("partial indexed report: " + resp.documents_stats.created + " created, " + resp.documents_stats.updated + " updated, " + resp.documents_stats.error + " errors. time: " + resp.time_stats.total + "/" + resp.time_stats.elasticsearch)
    } catch {
      case HttpException(code, req, resp) =>
        throw DGSearcherDaoException(code + ": " + resp + "(request: " + req + ")")
      case e: Throwable =>
        LOG.error("error while indexPartial models", e)
        throw DGSearcherDaoException(e.getMessage)
    }
  }

  @throws(classOf[DGSearcherDaoException])
  def indexTotal(model:  String, doc: String, token: String): Unit = {
    try {
      LOG.debug("indexing total ...")
      val result: String = httpManager.totalPostRequest(model, token, doc)
      val json = parse(result)
      val resp: Indexer  = json.extract[Indexer]
      LOG.debug("total indexed report: " + resp.documents_stats.created + " created, " + resp.documents_stats.updated + " updated, , " + resp.documents_stats.error + " errors. time: " + resp.time_stats.total + "/" + resp.time_stats.elasticsearch)
    } catch {
      case HttpException(code, req, resp) =>
        throw DGSearcherDaoException(code + ": " + resp + "(request: " + req + ")")
      case e: Throwable =>
        LOG.error("error while indexTotal models", e)
        throw DGSearcherDaoException(e.getMessage)
    }
  }

  @throws(classOf[DGSearcherDaoException])
  override def getModels(): List[String] = {
    try {
      LOG.debug("getting models ...")
      val init = System.currentTimeMillis()
      val result: String = httpManager.getManagerModels()
      val json = parse(result)
      val domains: ManagerDomains = json.extract[ManagerDomains]
      LOG.debug("getting models: " + domains + ". time elapsed: " + (System.currentTimeMillis() - init))
      domains.domains.map( d => d.id )
    } catch {
      case HttpException(code, req, resp) =>
        throw DGSearcherDaoException(code + ": " + resp + "(request: " + req + ")")
      case e: Throwable =>
        LOG.error("error while geting models", e)
        throw DGSearcherDaoException(e.getMessage)
    }
  }

  @throws(classOf[DGSearcherDaoException])
  override def checkTotalIndexation(model: String): (Boolean, Option[String]) = {
    try {
      LOG.debug("checking Total Indexation ...")
      val init = System.currentTimeMillis()
      val result: String = httpManager.getIndexerdomains()
      val json = parse(result)
      val domains: IndexerDomains = json.extract[IndexerDomains]
      val domain: List[IndexerDomain] = domains.domains.filter(d => d.domain.contains(model))
      if (domain.isEmpty) {
        (false, None)
      } else {
        val dom: IndexerDomain = domain.head
        if (dom.status.isEmpty || dom.status.contains(FINAL_STATUS_OK) || dom.status.contains(FINAL_STATUS_NOK)) {
          LOG.debug("Total Indexation checked FALSE. time elapsed: " + (System.currentTimeMillis()-init))
          (false, None)
        } else {
          LOG.debug("Total Indexation checked TRUE. time elapsed: " + (System.currentTimeMillis()-init))
          (true, dom.token)
        }
      }
    } catch {
      case HttpException(code, req, resp) =>
        throw DGSearcherDaoException(code + ": " + resp + "(request: " + req + ")")
      case e: Throwable =>
        LOG.error("error while checking Total Indexation", e)
        throw DGSearcherDaoException(e.getMessage)
    }
  }

  @throws(classOf[DGSearcherDaoException])
  override def insertModel(model: String, jsonModel: String): Unit = {
    try {
      LOG.debug("inserting model " + model + " ... ")
      val init = System.currentTimeMillis()
      httpManager.insertOrUpdateModel(model, jsonModel)
      LOG.debug("model inserted. time elapsed: " + (System.currentTimeMillis()-init))
    } catch {
      case HttpException(code, req, resp) =>
        throw DGSearcherDaoException(code + ": " + resp + "(request: " + req + ")")
      case e: Throwable =>
        LOG.error("error while inserting Or updating Model", e)
        throw DGSearcherDaoException(e.getMessage)
    }
  }

  @throws(classOf[DGSearcherDaoException])
  override def initTotalIndexationProcess(model: String): String = {
    try {
      LOG.debug("initiating total indexation process for model " + model + " ... ")
      val init = System.currentTimeMillis()
      val result: String = httpManager.initTotalIndexationProcess(model)
      val json = parse(result)
      val domain: IndexerDomain = json.extract[IndexerDomain]

      if (domain.status.contains(INDEXING_STATUS)) {
        LOG.debug("total indexing for model " + model + " and token: " + domain.token.get + ". time elapsed: " + (System.currentTimeMillis()-init))
        domain.token.get
      } else {
        throw DGSearcherDaoException("status is not " + INDEXING_STATUS)
      }
    } catch {
      case HttpException(code, req, resp) =>
        throw DGSearcherDaoException(code + ": " + resp + "(request: " + req + ")")
      case e: Throwable =>
        LOG.error("error while inserting Or updating Model", e)
        throw DGSearcherDaoException(e.getMessage)
    }
  }

  @throws(classOf[DGSearcherDaoException])
  override def finishTotalIndexationProcess(model: String, token: String): Unit = {
    try {
      LOG.debug("finishing total indexation process for model " + model + " and token " + token +" ... ")
      val init = System.currentTimeMillis()
      httpManager.finishTotalIndexationProcess(model, token)
      LOG.debug("total indexation  finished for model " + model + " and token " + token + ". time elapsed: " + (System.currentTimeMillis()-init))
    } catch {
      case HttpException(code, req, resp) =>
        throw DGSearcherDaoException(code + ": " + resp + "(request: " + req + ")")
      case e: Throwable =>
        LOG.error("error while inserting Or updating Model", e)
        throw DGSearcherDaoException(e.getMessage)
    }
  }

  @throws(classOf[DGSearcherDaoException])
  override def cancelTotalIndexationProcess(model: String, token: String): Unit = {
    try {
      LOG.debug("canceling total indexation process for model " + model + " and token " + token +" ... ")
      val init = System.currentTimeMillis()
      httpManager.cancelTotalIndexationProcess(model, token)
      LOG.debug("total indexation canceled for model " + model + " and token " + token + ". time elapsed: " + (System.currentTimeMillis()-init))
    } catch {
      case HttpException(code, req, resp) =>
        throw DGSearcherDaoException(code + ": " + resp + "(request: " + req + ")")
      case e: Throwable =>
        LOG.error("error while inserting Or updating Model", e)
        throw DGSearcherDaoException(e.getMessage)
    }
  }

}

case class DGSearcherDaoException(message: String) extends Throwable(message)

case class ManagerDomain(id: String, name: String)
case class ManagerDomains(total: Int, domains: List[ManagerDomain])
case class IndexerDomain(domain: Option[String], token: Option[String], status: Option[String], last_status_change: Option[String])
case class IndexerDomains(total_time_elapsed: Int, domains: List[IndexerDomain])

case class IndexerTimeStats(total: Int, elasticsearch: Int)
case class IndexerDocStats(created: Int, updated: Int, error: Int)
case class Indexer(time_stats: IndexerTimeStats, documents_stats: IndexerDocStats)
