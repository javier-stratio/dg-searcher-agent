package com.stratio.governance.agent.searcher.model

abstract class EntityRow(id: Long) {

  def getId(): Long = {
    return id
  }

}