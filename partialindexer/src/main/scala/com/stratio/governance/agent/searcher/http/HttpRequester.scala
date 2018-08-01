package com.stratio.governance.agent.searcher.http

import org.apache.http.client.methods.{CloseableHttpResponse, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClientBuilder

object HttpRequester {

  def partialPostRequest(json: String): CloseableHttpResponse = {

    //http://${indexer_base_path}/domain/partial
    val post = new HttpPost("http://localhost:8080/indexer/test_domain/partial")
    post.setHeader("Content-type", "application/json")
    post.setEntity(new StringEntity(json))

    val client = HttpClientBuilder.create.build
    client.execute(post)

  }
}
