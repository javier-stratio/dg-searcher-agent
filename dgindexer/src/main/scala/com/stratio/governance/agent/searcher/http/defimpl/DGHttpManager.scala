package com.stratio.governance.agent.searcher.http.defimpl

import com.stratio.governance.agent.searcher.http.{HttpException, HttpManager}
import org.apache.http.client.methods._
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.util.EntityUtils
import org.slf4j.{Logger, LoggerFactory}

class DGHttpManager(managerURL: String, indexerURL: String, ssl: Boolean) extends HttpManager {

  private lazy val LOG: Logger = LoggerFactory.getLogger(getClass.getName)
  lazy val protocol: String = if (ssl) "https" else "http"

  val MODEL: String = "{{model}}"
  val TOKEN: String = "{{token}}"
  val GET_MANAGER_MODELS_URL: String = protocol + "://" + managerURL + "/manager/domains"
  val PUT_MANAGER_MODEL: String = protocol + "://" + managerURL + "/manager/domains/" + MODEL
  val PUT_PARTIAL_INDEXATION: String = protocol + "://" + indexerURL + "/indexer/domains/" + MODEL + "/partial"
  val PUT_TOTAL_INDEXATION: String = protocol + "://" + indexerURL + "/indexer/domains/" + MODEL + "/total/" + TOKEN + "/index"
  val GET_INDEXER_DOMAINS: String = protocol + "://" + indexerURL + "/indexer/domains"
  val POST_TOTAL_INDEXATION_INIT: String = protocol + "://" + indexerURL + "/indexer/domains/" + MODEL + "/total"
  val PUT_TOTAL_INDEXATION_FINISH: String = protocol + "://" + indexerURL + "/indexer/domains/" + MODEL + "/total/" + TOKEN + "/end"
  val PUT_TOTAL_INDEXATION_CANCEL: String = protocol + "://" + indexerURL + "/indexer/domains/" + MODEL + "/total/" + TOKEN + "/cancel"

  @throws(classOf[HttpException])
  override def partialPostRequest(model: String, json: String): String = {
    val put = new HttpPut(PUT_PARTIAL_INDEXATION.replace(MODEL, model))
    put.setHeader("Content-type", "application/json")
    put.setEntity(new StringEntity(json))
    val response: HttpSearchResponse = handleHttpSearchRequest(put)
    val responseStr: String = response match {
      case HttpSearchResponseOK(_, message) => message
      case HttpSearchResponseKO(code, req, resp) => throw HttpException(code.toString, req, resp)
    }
    responseStr
  }

  @throws(classOf[HttpException])
  override def totalPostRequest(model: String, token: String, json: String): String = {
    val put = new HttpPut(PUT_TOTAL_INDEXATION.replace(MODEL, model).replace(TOKEN,token))
    put.setHeader("Content-type", "application/json")
    put.setEntity(new StringEntity(json))
    val response: HttpSearchResponse = handleHttpSearchRequest(put)
    val responseStr: String = response match {
      case HttpSearchResponseOK(_, message) => message
      case HttpSearchResponseKO(code, req, resp) => throw HttpException(code.toString, req, resp)
    }
    responseStr
  }

  @throws(classOf[HttpException])
  override def getManagerModels(): String = {
    val get = new HttpGet(GET_MANAGER_MODELS_URL)
    val response: HttpSearchResponse = handleHttpSearchRequest(get)
    val responseStr: String = response match {
      case HttpSearchResponseOK(_, message) => message
      case HttpSearchResponseKO(code, req, resp) => throw HttpException(code.toString, req, resp)
    }
    responseStr
  }

  @throws(classOf[HttpException])
  override def getIndexerdomains(): String = {
    val get = new HttpGet(GET_INDEXER_DOMAINS)
    val response: HttpSearchResponse = handleHttpSearchRequest(get)
    val responseStr: String = response match {
      case HttpSearchResponseOK(_, message) => message
      case HttpSearchResponseKO(code, req, resp) => throw HttpException(code.toString, req, resp)
    }
    responseStr
  }

  @throws(classOf[HttpException])
  override def initTotalIndexationProcess(model: String): String = {
    val post = new HttpPost(POST_TOTAL_INDEXATION_INIT.replace(MODEL, model))
    val response: HttpSearchResponse = handleHttpSearchRequest(post)
    val responseStr: String = response match {
      case HttpSearchResponseOK(_, message) => message
      case HttpSearchResponseKO(code, req, resp) => throw HttpException(code.toString, req, resp)
    }
    responseStr
  }

  @throws(classOf[HttpException])
  override def insertOrUpdateModel(model: String, json: String): Unit = {
    val put = new HttpPut(PUT_MANAGER_MODEL.replace(MODEL, model))
    put.setHeader("Content-type", "application/json")
    put.setEntity(new StringEntity(json))

    val response: HttpSearchResponse = handleHttpSearchRequest(put)
    response match {
      case HttpSearchResponseOK(_,_) => // Nothing to do
      case HttpSearchResponseKO(code, req, resp) => throw HttpException(code.toString, req, resp)

    }
  }

  @throws(classOf[HttpException])
  override def finishTotalIndexationProcess(model: String, token: String): Unit = {
    val put = new HttpPut(PUT_TOTAL_INDEXATION_FINISH.replace(MODEL,model).replace(TOKEN,token))
    val response: HttpSearchResponse = handleHttpSearchRequest(put)
    response match {
      case HttpSearchResponseOK(_, _) => // Nothing to do
      case HttpSearchResponseKO(code, req, resp) => throw HttpException(code.toString, req, resp)
    }
  }

  @throws(classOf[HttpException])
  override def cancelTotalIndexationProcess(model: String, token: String): Unit = {
    val put = new HttpPut(PUT_TOTAL_INDEXATION_CANCEL.replace(MODEL,model).replace(TOKEN,token))
    val response: HttpSearchResponse = handleHttpSearchRequest(put)
    response match {
      case HttpSearchResponseOK(_, _) => // Nothing to do
      case HttpSearchResponseKO(code, req, resp) => throw HttpException(code.toString, req, resp)
    }
  }

  @throws(classOf[HttpException])
  private def handleHttpSearchRequest(request: HttpRequestBase): HttpSearchResponse = {
    try {
      val client = HttpClientBuilder.create.build
      val response: CloseableHttpResponse = client.execute(request)

      val code: Int = response.getStatusLine.getStatusCode

      if ((code >= 200) && (code < 300)) {
        HttpSearchResponseOK(request.getURI.toString, EntityUtils.toString(response.getEntity, "UTF-8"))
      } else {
        HttpSearchResponseKO(code, request.getURI.toString, EntityUtils.toString(response.getEntity, "UTF-8"))
      }
    } catch {
      case e: Throwable => {
        LOG.error("Error while processing http request", e)
        throw HttpException("000","",e.getMessage)
      }
    }
  }

}

abstract class HttpSearchResponse()
case class HttpSearchResponseOK(request: String, response: String) extends HttpSearchResponse
case class HttpSearchResponseKO(code: Int, request: String, response: String) extends HttpSearchResponse

