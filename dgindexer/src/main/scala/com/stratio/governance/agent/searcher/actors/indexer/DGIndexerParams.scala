package com.stratio.governance.agent.searcher.actors.indexer

import com.stratio.governance.agent.searcher.actors.dao.postgres.SourceDao

import com.stratio.governance.agent.searcher.actors.indexer.dao.SearcherDao

class DGIndexerParams(sourceDao: SourceDao, searcherDao: SearcherDao, partition: Int) extends IndexerParams {

  override def getPartition: Int =
    partition

  override def getSourceDao: SourceDao =
    sourceDao

  override def getSearcherDao: SearcherDao =
    searcherDao
}

