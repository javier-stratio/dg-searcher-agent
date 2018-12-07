package com.stratio.governance.agent.searcher.actors.indexer

import com.stratio.governance.agent.searcher.actors
import com.stratio.governance.agent.searcher.actors.indexer.dao.SearcherDao

class DGIndexerParams extends IndexerParams {

  override def getPartition(): Int = ???

  override def getSourceDao(): actors.dao.SourceDao = ???

  override def getSearcherDao(): SearcherDao = ???

}