package com.stratio.governance.agent.searcher.actors.extractor.dao

import com.stratio.governance.agent.searcher.actors.dao.postgres.PostgresPartialIndexationReadState
import com.stratio.governance.agent.searcher.model.es.ElasticObject

trait ReaderElementDao {

  // Read info for total indexation. Extract the whole indexation object
  def readElementsSince(offset: Int, limit: Int): (Array[ElasticObject], Int)

  // Read info for partial indexation. Extract only the ids
  def readUpdatedElementsIdsSince(state: PostgresPartialIndexationReadState): (List[String], List[Int],List[Int], PostgresPartialIndexationReadState)

  // Special method to extract Data Assets in partial indexation
  def readDataAssetsWhereMdpsIn(ids: List[String]): Array[ElasticObject]

  // Special method to extract Business Assets in partial indexation
  def readBusinessAssetsWhereIdsIn(ids: List[Int]): Array[ElasticObject]

  // Special method to extract Quality Rules in partial indexation
  def readQualityRulesWhereIdsIn(ids: List[Int]): Array[ElasticObject]

}
