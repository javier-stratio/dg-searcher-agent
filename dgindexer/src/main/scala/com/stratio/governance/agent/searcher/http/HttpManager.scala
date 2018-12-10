package com.stratio.governance.agent.searcher.http

import org.apache.http.client.methods.CloseableHttpResponse

trait HttpManager {

  @throws(classOf[HttpException])
  def getManagerModels(): String

  @throws(classOf[HttpException])
  def partialPostRequest(model: String, json: String): String

  @throws(classOf[HttpException])
  def totalPostRequest(model: String, token: String, json: String): String

  @throws(classOf[HttpException])
  def getIndexerdomains(): String

  @throws(classOf[HttpException])
  def initTotalIndexationProcess(model: String): String

  @throws(classOf[HttpException])
  def insertOrUpdateModel(model: String, json: String): Unit

  @throws(classOf[HttpException])
  def finishTotalIndexationProcess(model: String, token: String): Unit

  @throws(classOf[HttpException])
  def cancelTotalIndexationProcess(model: String, token: String): Unit

}

case class HttpException(code: String, request: String, response: String) extends Throwable(code + ": " + response + "(request: " + request + ")")
