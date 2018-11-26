package com.stratio.governance.agent.searcher.actors.extractor

import java.util.concurrent.TimeUnit

import akka.actor.Actor
import akka.pattern.CircuitBreaker
import com.typesafe.config.Config

import scala.concurrent.duration._

trait CircuitBreakerConfig {

  this: Actor =>

  val circuitBreakerConfig: Config

  lazy val maxFailures = circuitBreakerConfig.getInt("maxFailures")
  lazy val callTimeout = circuitBreakerConfig.getDuration("callTimeout.ms", TimeUnit.MILLISECONDS).millisecond
  lazy val resetTimeout = circuitBreakerConfig.getDuration("resetTimeout.ms", TimeUnit.MILLISECONDS).millisecond
  lazy val circuitBreaker = CircuitBreaker(context.system.scheduler, maxFailures, callTimeout, resetTimeout)

}
