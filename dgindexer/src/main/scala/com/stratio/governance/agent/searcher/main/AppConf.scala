package com.stratio.governance.agent.searcher.main

import com.typesafe.config.ConfigFactory

import scala.util.Try

trait AppConf {

  private val CIRCUIT_BREAKER_KEY = "circuitbreaker"
  private val CIRCUIT_BREAKER_DEFAULT_CONF =  "maxFailures = 5 \n" +
    "callTimeout.ms = 10000\n" +
    "resetTimeout.ms = 60000"

  private lazy val config = ConfigFactory.load

  lazy val circuitBreakerConf = Try(config.getConfig(CIRCUIT_BREAKER_KEY))
    .getOrElse(ConfigFactory.parseString(CIRCUIT_BREAKER_DEFAULT_CONF))
}
