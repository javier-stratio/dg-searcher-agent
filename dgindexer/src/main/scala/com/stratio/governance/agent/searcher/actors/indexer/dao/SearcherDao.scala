package com.stratio.governance.agent.searcher.actors.indexer.dao

trait SearcherDao {

  def index(doc: String): Unit

}
