package com.stratio.governance.agent.searcher.actors.indexer.dao

import com.stratio.governance.agent.searcher.model._
import com.stratio.governance.agent.searcher.model.es.EntityRowES

trait SourceDao {

  def keyValuePairProcess(keyValuePair: KeyValuePair): EntityRowES;

  def databaseSchemaProcess(databaseSchema: DatabaseSchema): EntityRowES;

  def fileTableProcess(fileTable: FileTable): EntityRowES;

  def fileColumnProcess(fileColumn: FileColumn): EntityRowES;

  def sqlTableProcess(sqlTable: SqlTable): EntityRowES;

  def sqlColumnProcess(sqlColumn: SqlColumn): EntityRowES;

}
