package com.stratio.governance.agent.search.testit

import com.stratio.governance.agent.searcher.actors.dao.postgres.PostgresSourceDao
import com.stratio.governance.agent.searcher.main.AppConf
import com.stratio.governance.agent.searcher.model.utils.ExponentialBackOff
import org.scalatest.FlatSpec

class PostgresSourceDaoITTest extends FlatSpec {


  "PostgresDao constructor " should " create all tables if is not " in {
    val database = "postgresDaoTest1"
    val schema : String = "schema"
    val exponentialBackOff :ExponentialBackOff = ExponentialBackOff(AppConf.extractorExponentialbackoffPauseMs, AppConf.extractorExponentialbackoffMaxErrorRetry)
    val postgresDao: PostgresSourceDao = new PostgresSourceDao(AppConf.sourceConnectionUrl, AppConf.sourceConnectionUser, AppConf.sourceConnectionPassword, database, schema, 1, 4, exponentialBackOff,true)
    
  }

}
