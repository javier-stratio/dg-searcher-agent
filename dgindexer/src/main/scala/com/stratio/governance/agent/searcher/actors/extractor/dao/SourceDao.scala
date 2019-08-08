package com.stratio.governance.agent.searcher.actors.extractor.dao

import java.sql.{PreparedStatement, ResultSet}

import com.stratio.governance.agent.searcher.actors.dao.postgres.PostgresPartialIndexationReadState

trait SourceDao {

  def close():Unit

  def readPartialIndexationState(): PostgresPartialIndexationReadState

  def writePartialIndexationState(state: PostgresPartialIndexationReadState): Unit

  def prepareStatement(queryName: String): PreparedStatement

  def execute(sql: String): Unit

  def executePreparedStatement(sql: PreparedStatement): Unit

  def executeQuery(sql: String): ResultSet

  def executeQueryPreparedStatement(sql: PreparedStatement): ResultSet

}
