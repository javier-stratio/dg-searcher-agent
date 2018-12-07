package com.stratio.governance.agent.searcher.http.defimpl

import com.stratio.governance.agent.searcher.http.{HttpException, HttpManager}
import org.apache.http.client.methods.CloseableHttpResponse

class DGHttpManager extends HttpManager {

  @throws(classOf[HttpException])
  override def partialPostRequest(json: String): CloseableHttpResponse = {
    // PUT http://localhost:8082/indexer/domains/governance_search_v0_4/partial
    /* in: <batch document to index>
       out: <resume>
      */
    null
  }

  @throws(classOf[HttpException])
  override def totalPostRequest(json: String, token: String): CloseableHttpResponse = {
    // PUT http://localhost:8082/indexer/domains/governance_search_v0_4/total TODO Check this
    /* in: <batch document to index>
       out: <resume>
      */
    null
  }

  override def getManagerModels(): String = {
    // GET http://localhost:8080/manager/domains
    /* out:
        {
            "total": 1,
            "domains": [
                {
                    "id": "governance_search_v0_4",
                    "name": "Governance Search V0.4"
                }
            ]
        }
     */
    ""
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
  override def insertOrUpdateModel(model: String, json: String): CloseableHttpResponse = {
    // PUT http://localhost:8080/manager/domains/governance_search_v0_4
    // in: Json Model
    // out: Nothing. Just Code
    null
  }

  @throws(classOf[HttpException])
  override def finishTotalIndexationProcess(model: String, token: String): CloseableHttpResponse = {
    // PUT http://localhost:8082/indexer/domains/governance_search_v0_4/total/d8e8425c-8df6-4e2e-80c3-ee05887a5357/end
    // in: Json Model
    // out: Nothing. Just Code
    null
  }

  @throws(classOf[HttpException])
  override def cancelTotalIndexationProcess(model: String, token: String): CloseableHttpResponse = {
    // PUT http://localhost:8082/indexer/domains/governance_search_v0_4/total/d8e8425c-8df6-4e2e-80c3-ee05887a5357/end
    // in: Json Model
    // out: Nothing. Just Code
    null
  }

}
