package com.stratio.governance.agent.searcher.actors.dao.postgres

import java.sql.Connection

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}

class Datasource(url: String, driverName: String, userName: String, password: String, maximumPoolSize: Int) {

  val ds: HikariDataSource = init

  def init: HikariDataSource = {
    val config = new HikariConfig()
    config.setJdbcUrl(url)
    config.setDriverClassName(driverName)
    config.setUsername(userName)
    config.setPassword(password)
    config.setMaximumPoolSize(maximumPoolSize)
    new HikariDataSource(config)
  }

  def getConnection: Connection = {
    ds.getConnection
  }
}

object Datasource {
  def getDataSource(url: String, driverName: String, userName: String, password: String, maximumPoolSize: Int): Datasource = {
    new Datasource(url, driverName, userName, password, maximumPoolSize)
  }
}