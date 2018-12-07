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
  implicit val formats = DefaultFormats

  val FINAL_STATUS_OK: String = "ENDED"
  val FINAL_STATUS_NOK: String = "CANCELLED"
  val INDEXING_STATUS: String = "INDEXING"

  override def index(doc: String): Unit = ???

  @throws(classOf[DGSearcherDaoException])
  override def getModels(): List[String] = {
    try {
      val result: String = httpManager.getManagerModels()
      val json = parse(result)
      val domains: ManagerDomains = json.extract[ManagerDomains]
      domains.domains.map( d => d.id )
    } catch {
      case HttpException(code, message) =>
        throw DGSearcherDaoException(code + ": " + message)
      case e: Throwable =>
        LOG.error("error while geting models", e)
        throw DGSearcherDaoException(e.getMessage)
    }
  }

  @throws(classOf[DGSearcherDaoException])
  override def checkTotalIndexation(model: String): (Boolean, Option[String]) = {
    try {
      val result: String = httpManager.getIndexerdomains()
      val json = parse(result)
      val domains: IndexerDomains = json.extract[IndexerDomains]
      val domain: List[IndexerDomain] = domains.domains.filter(d => d.domain == Some(model))
      if (domain.isEmpty) {
        (false, None)
      } else {
        val dom: IndexerDomain = domain.head
        if ((dom.status.isEmpty) || dom.status == Some(FINAL_STATUS_OK) || dom.status == Some(FINAL_STATUS_NOK))
          (false, None)
        else
          (true, dom.token)
      }
    } catch {
      case HttpException(code, message) =>
        throw DGSearcherDaoException(code + ": " + message)
      case e: Throwable =>
        LOG.error("error while checking Total Indexation", e)
        throw DGSearcherDaoException(e.getMessage)
    }
  }

  @throws(classOf[DGSearcherDaoException])
  override def insertModel(model: String, jsonModel: String): Unit = {
    try {
      httpManager.insertOrUpdateModel(model, jsonModel)
    } catch {
      case HttpException(code, message) =>
        throw DGSearcherDaoException(code + ": " + message)
      case e: Throwable =>
        LOG.error("error while inserting Or updating Model", e)
        throw DGSearcherDaoException(e.getMessage)
    }
  }

  @throws(classOf[DGSearcherDaoException])
  override def initTotalIndexationProcess(model: String): String = {
    try {
      val result: String = httpManager.initTotalIndexationProcess(model)
      val json = parse(result)
      val domain: IndexerDomain = json.extract[IndexerDomain]

      if (domain.status == Some(INDEXING_STATUS)) {
        return domain.token.get
      } else {
        throw DGSearcherDaoException("status is not " + INDEXING_STATUS)
      }
    } catch {
      case HttpException(code, message) =>
        throw DGSearcherDaoException(code + ": " + message)
      case e: Throwable =>
        LOG.error("error while inserting Or updating Model", e)
        throw DGSearcherDaoException(e.getMessage)
    }
  }

  @throws(classOf[DGSearcherDaoException])
  override def finishTotalIndexationProcess(model: String, token: String): Unit = {
    try {
      httpManager.finishTotalIndexationProcess(model, token)
    } catch {
      case HttpException(code, message) =>
        throw DGSearcherDaoException(code + ": " + message)
      case e: Throwable =>
        LOG.error("error while inserting Or updating Model", e)
        throw DGSearcherDaoException(e.getMessage)
    }
  }

  @throws(classOf[DGSearcherDaoException])
  override def cancelTotalIndexationProcess(model: String, token: String): Unit = {
    try {
      httpManager.cancelTotalIndexationProcess(model, token)
    } catch {
      case HttpException(code, message) =>
        throw DGSearcherDaoException(code + ": " + message)
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
