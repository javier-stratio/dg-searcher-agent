package com.stratio.governance.agent.searcher.actors.indexer

import com.stratio.governance.agent.searcher.actors.indexer.dao.{SearcherDao, SourceDao}

trait IndexerParams {

  def getPartiton(): Int
  def getSourceDao(): SourceDao
  def getSearcherDao(): SearcherDao

}
