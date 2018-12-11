package com.stratio.governance.agent.search.testit.utils

import com.stratio.governance.agent.searcher.main.AppConf

import scala.util.Properties

object Configuration {
  private def getPropertyOrDefault(path: String, defaultValue: String): String = Properties.envOrNone("it."+ path).getOrElse(defaultValue)


  val MANAGER_URL: String = getPropertyOrDefault("manager_url","http://localhost:8080")
  val INDEXER_URL: String = getPropertyOrDefault("indexer_url","http://localhost:8082")

  val POSTGRES_URL: String = AppConf.sourceConnectionUrl

  val MODEL: String = "governance_search_test"
}
