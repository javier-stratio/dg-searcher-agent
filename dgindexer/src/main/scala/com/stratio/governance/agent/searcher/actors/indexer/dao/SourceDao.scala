package com.stratio.governance.agent.searcher.actors.indexer.dao

import com.stratio.governance.agent.searcher.model.EntityRow

trait SourceDao {

  def keyValuePairProcess(mdps: List[String]): List[EntityRow]

  def businessAssets(mdps: List[String]): List[EntityRow]
}
