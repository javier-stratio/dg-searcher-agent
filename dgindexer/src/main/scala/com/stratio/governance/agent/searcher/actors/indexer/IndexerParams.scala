package com.stratio.governance.agent.searcher.actors.indexer

import com.stratio.governance.agent.searcher.actors.dao.SourceDao
import com.stratio.governance.agent.searcher.actors.indexer.dao.SearcherDao

trait IndexerParams {

  def getPartition(): Int
  def getSourceDao(): SourceDao
  def getSearcherDao(): SearcherDao

}
