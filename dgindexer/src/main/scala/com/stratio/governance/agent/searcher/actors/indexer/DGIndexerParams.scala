package com.stratio.governance.agent.searcher.actors.indexer

import com.stratio.governance.agent.searcher.actors.dao.SourceDao
import com.stratio.governance.agent.searcher.actors.indexer.dao.SearcherDao
//import com.stratio.governance.agent.searcher.model.utils.KeyValuePairMapping

class DGIndexerParams(sourceDao: SourceDao, searcherDao : SearcherDao) extends IndexerParams {


  override def getSourceDao(): SourceDao = {
    sourceDao
  }

  override def getSearcherDao(): SearcherDao = {
    searcherDao
  }

  override def getPartition(): Int = {
    // TODO CONFIG
    10
  }
}
