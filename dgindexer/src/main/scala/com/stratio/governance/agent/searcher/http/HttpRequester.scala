package com.stratio.governance.agent.searcher.http

import com.stratio.governance.agent.searcher.model.utils.JsonUtils
import org.apache.http.client.methods.{CloseableHttpResponse, HttpPost, HttpPut}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.util.EntityUtils

@Deprecated
object HttpRequester {

  def partialPostRequest(json: String): CloseableHttpResponse = {

    //http://${indexer_base_path}/domain/partial
    val post = new HttpPost("http://localhost:8081/indexer/test_domain/partial")
    post.setHeader("Content-type", "application/json")
    post.setEntity(new StringEntity(json))

    val client = HttpClientBuilder.create.build
    client.execute(post)
  }

  def totalPostRequest(json: String): CloseableHttpResponse = {

    //http://${indexer_base_path}/domain/partial
    val initTotal = new HttpPost("http://localhost:8081/indexer/test_domain/total")
    //post.setHeader("Content-type", "application/json")
    //post.setEntity(new StringEntity(json))

    val client = HttpClientBuilder.create.build
    val responseTotal: CloseableHttpResponse = client.execute(initTotal)

    import org.apache.http.HttpEntity
    val entity = responseTotal.getEntity
    val responseTotalString = EntityUtils.toString(entity, "UTF-8")
    println(s"responseString :: $responseTotalString")

    val responseTotalMap = JsonUtils.jsonStrToMap(responseTotalString)
    val token = responseTotalMap("token")

    //http://localhost:8081/indexer/test_domain/total/{{token}}/index
    val putDocuments = new HttpPut(s"http://localhost:8081/indexer/test_domain/total/$token/index")
    putDocuments.setHeader("Content-type", "application/json")
    putDocuments.setEntity(new StringEntity(json))
    val responseDocuments: CloseableHttpResponse = client.execute(putDocuments)
    val entityDocuments = responseDocuments.getEntity
    val responseDocumentsString = EntityUtils.toString(entityDocuments, "UTF-8")
    println(s"responseString :: $responseDocumentsString")

    val endTotalPut = new HttpPut(s"http://localhost:8081/indexer/test_domain/total/$token/end")
    val finalResponse = client.execute(endTotalPut)
    val entityFinalResponse = finalResponse.getEntity
    val responseFinalResponse = EntityUtils.toString(entityFinalResponse, "UTF-8")
    println(s"responseFinalResponse :: $responseFinalResponse")

    responseDocuments
  }

}
