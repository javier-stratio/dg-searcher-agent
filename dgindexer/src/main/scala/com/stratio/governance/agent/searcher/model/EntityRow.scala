package com.stratio.governance.agent.searcher.model

abstract class EntityRow(metadataPath: String) {
  def getMatadataPath: String = metadataPath
}