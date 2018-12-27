package com.stratio.governance.agent.searcher.actors.indexer

import com.stratio.governance.agent.searcher.actors.indexer.dao.SourceDao
import com.stratio.governance.agent.searcher.actors.indexer.dao.SearcherDao
import com.stratio.governance.agent.searcher.actors.utils.AdditionalBusiness

class DGIndexerParams(sourceDao: SourceDao, searcherDao: SearcherDao, partition: Int, additionalBusiness: AdditionalBusiness) extends IndexerParams {

  override def getPartition: Int =
    partition

  override def getSourceDao: SourceDao =
    sourceDao

  override def getSearcherDao: SearcherDao =
    searcherDao

  override def getAdditionalBusiness: AdditionalBusiness =
    additionalBusiness

}

