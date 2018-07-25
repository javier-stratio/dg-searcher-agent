package com.stratio.governance.agent.searcher.model.es

case class CategoryES(id: String,
                      name: String)

object CategoryES{

  //TODO all these functions are identical. Merge into one and test them

  def fromDatastoreEngineGeneratedId(generatedID: String): CategoryES = {
    CategoryES(generatedID.split("/").take(1).mkString("/"),
               generatedID.split("/").take(1).mkString("/"))

  }

  def fromFileTableGeneratedId(generatedID: String): CategoryES = {
    CategoryES(generatedID.split("/").take(1).mkString("/"),
      generatedID.split("/").take(1).mkString("/"))

  }

  def fromDatabaseSchemaGeneratedId(generatedID: String): CategoryES = {
    CategoryES(generatedID.split("/").take(1).mkString("/"),
      generatedID.split("/").take(1).mkString("/"))

  }

  def fromFileColumnGeneratedId(generatedID: String): CategoryES = {
    CategoryES(generatedID.split("/").take(1).mkString("/"),
      generatedID.split("/").take(1).mkString("/"))

  }

  def fromSqlColumnGeneratedId(generatedID: String): CategoryES = {
    CategoryES(generatedID.split("/").take(1).mkString("/"),
      generatedID.split("/").take(1).mkString("/"))

  }

  def fromSqlTableGeneratedId(generatedID: String): CategoryES = {
    CategoryES(generatedID.split("/").take(1).mkString("/"),
      generatedID.split("/").take(1).mkString("/"))

  }

}