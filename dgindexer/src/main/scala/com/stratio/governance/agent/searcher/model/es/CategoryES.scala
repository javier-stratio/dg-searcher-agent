package com.stratio.governance.agent.searcher.model.es

case class CategoryES(id: String,
                      name: String)

object CategoryES{

  //TODO all these functions are identical. Merge into one and test them
  //indexer component requests thr Category as an array of arrays. This is why a Seq[CategoryES] is returned.
  //TODO instead of that, CategoryES should be a Seq of id & name elements

  def fromDatastoreEngineGeneratedId(generatedID: String): Seq[CategoryES] = {
    Seq(CategoryES(generatedID.split("/").take(1).mkString("/"),
      generatedID.split("/").take(1).mkString("/")))

  }

  def fromFileTableGeneratedId(generatedID: String): Seq[CategoryES] = {
    Seq(CategoryES(generatedID.split("/").take(1).mkString("/"),
      generatedID.split("/").take(1).mkString("/")))

  }

  def fromDatabaseSchemaGeneratedId(generatedID: String): Seq[CategoryES] = {
    Seq(CategoryES(generatedID.split("/").take(1).mkString("/"),
      generatedID.split("/").take(1).mkString("/")))

  }

  def fromFileColumnGeneratedId(generatedID: String): Seq[CategoryES] = {
    Seq(CategoryES(generatedID.split("/").take(1).mkString("/"),
      generatedID.split("/").take(1).mkString("/")))

  }

  def fromSqlColumnGeneratedId(generatedID: String): Seq[CategoryES] = {
    Seq(CategoryES(generatedID.split("/").take(1).mkString("/"),
      generatedID.split("/").take(1).mkString("/")))

  }

  def fromSqlTableGeneratedId(generatedID: String): Seq[CategoryES] = {
    Seq(CategoryES(generatedID.split("/").take(1).mkString("/"),
      generatedID.split("/").take(1).mkString("/")))

  }

}