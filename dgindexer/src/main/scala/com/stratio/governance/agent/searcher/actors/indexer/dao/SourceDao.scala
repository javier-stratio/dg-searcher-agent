package com.stratio.governance.agent.searcher.actors.indexer.dao

import com.stratio.governance.agent.searcher.model.EntityRow

trait SourceDao {

  def keyValuePairProcess(ids: Array[Int]): List[EntityRow]

  def businessAssets(ids: Array[Int]): List[EntityRow]
}
