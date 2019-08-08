package com.stratio.governance.agent.searcher.model

abstract class EntityRow(identifier: String) {
  def getIdentifier: String = identifier
}