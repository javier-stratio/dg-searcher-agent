package com.stratio.governance.agent.searcher.main

import com.typesafe.config.ConfigFactory

import scala.util.Properties

object AppConf {

  private lazy val config = ConfigFactory.load

  private def envOrElseConfigOrElseDefault(path: String, defaultValue: Int): Int = {
    Properties.envOrNone(path.toUpperCase.replaceAll("""\.""", "_")).map(_.toInt)
      .getOrElse(if (config.hasPath(path)) config.getInt(path) else defaultValue)
  }

  private def envOrElseConfigOrElseDefault(path: String, defaultValue: Long): Long = {
    Properties.envOrNone(path.toUpperCase.replaceAll("""\.""", "_")).map(_.toLong)
      .getOrElse(if (config.hasPath(path)) config.getLong(path) else defaultValue)
  }

  private def envOrElseConfigOrElseDefault(path: String, defaultValue: String): String = {
    Properties.envOrNone(path.toUpperCase.replaceAll("""\.""", "_"))
      .getOrElse(if (config.hasPath(path)) config.getString(path) else defaultValue)
  }

  private def envOrElseConfigOrElseDefault(path: String, defaultValue: Boolean): Boolean = {
    Properties.envOrNone(path.toUpperCase.replaceAll("""\.""", "_")).map(_.toBoolean)
      .getOrElse(if (config.hasPath(path)) config.getBoolean(path) else defaultValue)
  }

  lazy val extractorLimit: Int = envOrElseConfigOrElseDefault("extractor.limit", 1000)

  lazy val extractorPeriodMs: Long = envOrElseConfigOrElseDefault("extractor.period.ms", 10000)

  lazy val extractorExponentialbackoffPauseMs: Long = envOrElseConfigOrElseDefault("extractor.exponentialbackoff.pause.ms", 1000)

  lazy val extractorExponentialbackoffMaxErrorRetry: Int = envOrElseConfigOrElseDefault("extractor.exponentialbackoff.max.error.retry", 5)

  lazy val extractorDelayMs: Long = envOrElseConfigOrElseDefault("extractor.delay.ms",1000)

  lazy val sourceDatabase: String = envOrElseConfigOrElseDefault("source.database", "dg_database")

  lazy val sourceSchema: String = envOrElseConfigOrElseDefault("source.schema", "dg_metadata")

  lazy val sourceConnectionInitialSize: Int = envOrElseConfigOrElseDefault("source.connection.initial.size", 1000)

  lazy val sourceConnectionMaxSize: Int = envOrElseConfigOrElseDefault("source.connection.max.size",1000)

  lazy val sourceConnectionUrl: String = envOrElseConfigOrElseDefault("source.connection.url","jdbc:postgresql://localhost:5432/hakama")

  lazy val sourceConnectionUser: String = envOrElseConfigOrElseDefault("source.connection.user","postgres")

  lazy val sourceConnectionPassword: String = envOrElseConfigOrElseDefault("source.connection.password","######")

  lazy val indexerPartition: Int = envOrElseConfigOrElseDefault("indexer.partition",1000)

  lazy val managerUrl: String = envOrElseConfigOrElseDefault("manager.manager.url","http://localhost:8080")

  lazy val indexerURL: String = envOrElseConfigOrElseDefault("manager.indexer.url","http://localhost:8082")

  lazy val schedulerPartialEnabled: Boolean = envOrElseConfigOrElseDefault("scheduler.partialIndexation.enabled",true)

  lazy val schedulerPartialInterval: Int = envOrElseConfigOrElseDefault("scheduler.partialIndexation.interval.s",10)

  lazy val schedulerTotalEnabled: Boolean = envOrElseConfigOrElseDefault("scheduler.totalIndexation.enabled",true)

  lazy val schedulerTotalCronExpresion: String = envOrElseConfigOrElseDefault("scheduler.totalIndexation.cron","*/30 * * ? * *")

}
