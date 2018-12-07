package com.stratio.governance.agent.searcher.http

import org.apache.http.client.methods.CloseableHttpResponse

trait HttpManager {

  @throws(classOf[HttpException])
  def getManagerModels(): String

  @throws(classOf[HttpException])
  def partialPostRequest(json: String): CloseableHttpResponse

  @throws(classOf[HttpException])
  def totalPostRequest(json: String, token: String): CloseableHttpResponse

  @throws(classOf[HttpException])
  def getIndexerdomains(): String

  @throws(classOf[HttpException])
  def initTotalIndexationProcess(model: String): String

  @throws(classOf[HttpException])
  def insertOrUpdateModel(model: String, json: String): CloseableHttpResponse

  @throws(classOf[HttpException])
  def finishTotalIndexationProcess(model: String, token: String): CloseableHttpResponse

  @throws(classOf[HttpException])
  def cancelTotalIndexationProcess(model: String, token: String): CloseableHttpResponse

}

case class HttpException(code: String, message: String) extends Throwable(code + ": " + message)