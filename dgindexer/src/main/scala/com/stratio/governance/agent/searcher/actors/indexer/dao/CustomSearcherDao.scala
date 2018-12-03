package com.stratio.governance.agent.searcher.actors.indexer.dao

import com.stratio.governance.agent.searcher.http.HttpRequester

class CustomSearcherDao extends SearcherDao {
  override def index(doc: String): Unit = {
    HttpRequester.partialPostRequest(doc)
  }
}
