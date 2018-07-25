package com.stratio.governance.agent.searcher.http

import com.stratio.governance.agent.searcher.model.utils.JsonUtils
import org.apache.http.client.methods.{CloseableHttpResponse, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.util.EntityUtils


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
    val token = responseTotalMap.get("token")

    //TODO token
    //http://localhost:8081/indexer/test_domain/total/{{token}}/index
    val postDocuments = new HttpPost(s"http://localhost:8081/indexer/test_domain/total/$token/index")
    postDocuments.setHeader("Content-type", "application/json")
    postDocuments.setEntity(new StringEntity(json))
    val responseDocuments: CloseableHttpResponse = client.execute(postDocuments)
    val entityDocuments = responseDocuments.getEntity
    val responseDocumentsString = EntityUtils.toString(entityDocuments, "UTF-8")
    println(s"responseString :: $responseTotalString")

    responseDocuments
  }

}
