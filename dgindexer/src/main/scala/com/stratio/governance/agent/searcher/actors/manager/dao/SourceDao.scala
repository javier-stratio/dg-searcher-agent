package com.stratio.governance.agent.searcher.actors.manager.dao

trait SourceDao {

  def getKeys(): List[String]

}
