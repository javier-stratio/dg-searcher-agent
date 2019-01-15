package com.stratio.governance.agent.search.testit.utils

import com.stratio.governance.agent.searcher.actors.dao.postgres.PostgresSourceDao
import com.stratio.governance.agent.searcher.actors.utils.AdditionalBusiness
import com.stratio.governance.agent.searcher.model.utils.ExponentialBackOff

class PostgresSourceDaoTest(sourceConnectionUrl: String,
                            sourceConnectionUser: String,
                            sourceConnectionPassword: String,
                            database:String,
                            schema: String,
                            initialSize: Int,
                            maxSize: Int,
                            exponentialBackOff: ExponentialBackOff,
                            additionalBusiness: AdditionalBusiness,
                            allowedToCreateContext: Boolean = false) extends PostgresSourceDao(sourceConnectionUrl, sourceConnectionUser, sourceConnectionPassword, database, schema, initialSize, maxSize, exponentialBackOff, additionalBusiness, allowedToCreateContext) {

  override def start(): Unit = {
    // Here we do not want to create database
  }

  def startParent(): Unit = {
    super.start()
  }

}
