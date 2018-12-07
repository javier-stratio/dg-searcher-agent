package com.stratio.governance.agent.searcher.actors.manager.dao

import com.stratio.governance.agent.searcher.actors.manager.utils.ManagerUtilsException

trait SearcherDao {

  @throws(classOf[ManagerUtilsException])
  def getModels(): List[String]

  @throws(classOf[ManagerUtilsException])
  def checkTotalIndexation(model: String): (Boolean, Option[String])

  @throws(classOf[ManagerUtilsException])
  def insertModel(model: String, jsonModel: String): Unit

  @throws(classOf[ManagerUtilsException])
  def initTotalIndexationProcess(model: String): String

  @throws(classOf[ManagerUtilsException])
  def finishTotalIndexationProcess(model: String, token: String): Unit

  @throws(classOf[ManagerUtilsException])
  def cancelTotalIndexationProcess(model: String, token: String): Unit

}
