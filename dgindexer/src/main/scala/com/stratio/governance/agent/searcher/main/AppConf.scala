package com.stratio.governance.agent.searcher.main

import com.typesafe.config.ConfigFactory

object AppConf {

  private lazy val config = ConfigFactory.load

  import scala.util.Properties

  private def envOrElseConfigOrElseDefault(path: String, defaultValue: Int): Int = {
    Properties.envOrNone(path.toUpperCase.replaceAll("""\.""", "_")).map(_.toInt)
      .getOrElse(if (config.hasPath(path)) config.getInt(path)  else  defaultValue)
  }

  private def envOrElseConfigOrElseDefault(path: String, defaultValue: Long): Long = {
    Properties.envOrNone(path.toUpperCase.replaceAll("""\.""", "_")).map(_.toLong)
      .getOrElse(if (config.hasPath(path)) config.getLong(path)  else  defaultValue)
  }

  private def envOrElseConfigOrElseDefault(path: String, defaultValue: String): String = {
    Properties.envOrNone(path.toUpperCase.replaceAll("""\.""", "_"))
      .getOrElse(if (config.hasPath(path)) config.getString(path)  else  defaultValue)
  }

  private def envOrElseConfig(path: String): Option[String] = {
    Properties.envOrNone(path.toUpperCase.replaceAll("""\.""", "_"))
      .orElse(Some(config.getString(path)))
  }

  lazy val extractorLimit: Int = envOrElseConfigOrElseDefault("extractor.limit", 1000)

  lazy val extractorPeriodMs: Long = envOrElseConfigOrElseDefault("extractor.period.ms", 10000)

  //lazy val extractorSchedulerMode: SchedulerMode = SchedulerMode.valueOf(envOrElseConfig("extractor.scheduler.mode")).getOrElse[SchedulerMode](SchedulerMode.Periodic)

  lazy val extractorExponentialbackoffPauseMs: Long = envOrElseConfigOrElseDefault("extractor.exponentialbackoff.pause.ms", 1000)

  lazy val extractorExponentialbackoffMaxErrorRetry: Int = envOrElseConfigOrElseDefault("extractor.exponentialbackoff.max.error.retry", 5)

  lazy val extractorDelayMs: Long = envOrElseConfigOrElseDefault("extractor.delay.ms",1000)

  lazy val sourceDatabase: String = envOrElseConfigOrElseDefault("source.database", "dg_metadata")

  lazy val sourceConnectionInitialSize: Int = envOrElseConfigOrElseDefault("source.connection.initial.size", 1000)

  lazy val sourceConnectionMaxSize: Int = envOrElseConfigOrElseDefault("source.connection.max.size",1000)

  lazy val sourceConnectionUrl: String =envOrElseConfigOrElseDefault("source.connection.url","jdbc:postgresql://localhost:5432/hakama")

  lazy val sourceConnectionUser: String = envOrElseConfigOrElseDefault("source.connection.user","postgres")

  lazy val sourceConnectionPassword: String = envOrElseConfigOrElseDefault("source.connection.password","######")

}
