package com.stratio.governance.agent.searcher.actors.indexer.dao

import com.stratio.governance.agent.searcher.model.EntityRow

trait SourceDao {

  // Additional data for DataAssets

  def keyValuePairForDataAsset(mdps: List[String]): List[EntityRow]

  def businessTermsForDataAsset(mdps: List[String]): List[EntityRow]

  def qualityRulesForDataAsset(mdps: List[String]): List[EntityRow]

  // Additional data for BusinessAssets

  def keyValuePairForBusinessAsset(ids: List[Long]): List[EntityRow]

  // Additional data for Quality Rules

  def keyValuePairForQualityRule(ids: List[Long]): List[EntityRow]

  def businessRulesForQualityRule(ids: List[Long]): List[EntityRow]
  
}
