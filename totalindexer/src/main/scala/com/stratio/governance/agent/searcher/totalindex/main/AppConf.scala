package com.stratio.governance.agent.searcher.totalindex.main

import com.typesafe.config.ConfigFactory

trait AppConf {

  private lazy val config = ConfigFactory.load

}
