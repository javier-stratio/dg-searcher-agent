package com.stratio.governance.agent.searcher.actors.indexer.dao

trait SearcherDao {

  def indexPartial(model:  String, doc: String): Unit

  def indexTotal(model:  String, doc: String, token: String): Unit

}
