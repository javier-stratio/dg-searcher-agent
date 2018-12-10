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
  val GET_MANAGER_MODELS_URL: String = protocol + "://" + managerURL + "/manager/domains"
  val PUT_MANAGER_MODEL: String = protocol + "://" + managerURL + "/manager/domains/" + MODEL


  @throws(classOf[HttpException])
  override def partialPostRequest(json: String): Unit = {
    // PUT http://localhost:8082/indexer/domains/governance_search_v0_4/partial
    /* in: <batch document to index>
       out: <resume>
      */
  }

  @throws(classOf[HttpException])
  override def totalPostRequest(json: String, token: String): Unit = {
    // PUT http://localhost:8082/indexer/domains/governance_search_v0_4/total TODO Check this
    /* in: <batch document to index>
       out: <resume>
      */
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
    //GET http://se-indexer.bootstrap.spaceai.hetzner.stratio.com/indexer/domains
   /* out:
   {
    "total_time_elapsed": 128,
    "domains": [
        {
            "domain": "governance_search_v0_3",
            "token": null,
            "status": "ENDED",
            "last_status_change": "2018-11-29T15:03:30.488+0000"
        },
        {
            "domain": "movies",
            "token": "3e33463a-2ed9-4c15-87c0-1a520e3c1cb2",
            "status": "ENDED",
            "last_status_change": "2018-11-29T14:58:46.240+0000"
        }
      ]
    }
    */
    ""
  }

  @throws(classOf[HttpException])
  override def initTotalIndexationProcess(model: String): String = {
    // POST http://localhost:8082/indexer/domains/governance_search_v0_4/total
    /* in: Nothing
       out:
      {
          "token": "dde76989-4725-4ba2-82fd-400b07d32b32",
          "status": "INDEXING",
          "last_status_change": "2018-12-05T07:54:12.034+0000",
          "time_stats": {
              "total": 218
          }
      }
     */
    ""
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
    // PUT http://localhost:8082/indexer/domains/governance_search_v0_4/total/d8e8425c-8df6-4e2e-80c3-ee05887a5357/end
    // in: Json Model
    // out: Nothing. Just Code
  }

  @throws(classOf[HttpException])
  override def cancelTotalIndexationProcess(model: String, token: String): Unit = {
    // PUT http://localhost:8082/indexer/domains/governance_search_v0_4/total/d8e8425c-8df6-4e2e-80c3-ee05887a5357/end
    // in: Json Model
    // out: Nothing. Just Code
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

