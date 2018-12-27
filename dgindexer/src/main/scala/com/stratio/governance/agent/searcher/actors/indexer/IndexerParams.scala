package com.stratio.governance.agent.searcher.actors.indexer

import com.stratio.governance.agent.searcher.actors.indexer.dao.SourceDao
import com.stratio.governance.agent.searcher.actors.indexer.dao.SearcherDao
import com.stratio.governance.agent.searcher.actors.utils.AdditionalBusiness

trait IndexerParams {
  def getPartition: Int
  def getSourceDao: SourceDao
  def getSearcherDao: SearcherDao
  def getAdditionalBusiness: AdditionalBusiness
}
