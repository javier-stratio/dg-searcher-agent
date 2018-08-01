package com.stratio.governance.agent.searcher.model.utils

import com.stratio.governance.agent.searcher.model.es.{CategoryES, GeneratedES, KeyValuePairES}
import com.stratio.governance.agent.searcher.model._

object GenerateESObject {

  def genParentId(generatedId: String): String = {
    val genIdToList = generatedId.split("/")
    val x = genIdToList.take(genIdToList.length - 2).mkString("/")
    x
  }

  def genGeneratedId(datastoreEngine: DatastoreEngine): String = {
    val x = s"/${datastoreEngine.`type`}/${datastoreEngine.id}"
    x
  }

  def genGeneratedId(sType: String, id: Int): String = {
    s"/$sType/$id"
  }

  def genGenerated(datastoreEngine: DatastoreEngine): GeneratedES = {
    val generatedId = GenerateESObject.genGeneratedId(datastoreEngine)
    GeneratedES(
      DatastoreEngine.entity,
      datastoreEngine.`type`,
      generatedId,
      GenerateESObject.genParentId(generatedId),
      Seq(CategoryES.fromGeneratedId(generatedId)),
      KeyValuePairES.fromDatastoreEngine(datastoreEngine)
    )
  }

  def genGenerated(databaseSchema: DatabaseSchema, keyValuePair: KeyValuePair): GeneratedES = {
    val generatedId = GenerateESObject.genGeneratedId("PostgreSQL", databaseSchema.id)
    GeneratedES(
      DatastoreEngine.entity,
      "PostgreSQL",
      generatedId,
      GenerateESObject.genParentId(generatedId),
      Seq(CategoryES.fromGeneratedId(generatedId)),
      KeyValuePairES.fromKeyValuePair(keyValuePair)
    )
  }

  def genGenerated(databaseSchema: DatabaseSchema,
                   databaseSchemaList: List[(DatabaseSchema, KeyValuePair)]): GeneratedES = {
    val generatedId = GenerateESObject.genGeneratedId("PostgreSQL", databaseSchema.id)
    GeneratedES(
      DatabaseSchema.entity,
      "PostgreSQL",
      generatedId,
      GenerateESObject.genParentId(generatedId),
      Seq(CategoryES.fromGeneratedId(generatedId)),
      KeyValuePairES.fromDatabaseSchemaList(databaseSchemaList)
    )
  }

  def genGenerated(fileTable: FileTable, fileTableList: List[(FileTable, KeyValuePair)]): GeneratedES = {
    val generatedId = GenerateESObject.genGeneratedId("PostgreSQL", fileTable.id)
    GeneratedES(
      FileTable.entity,
      "PostgreSQL",
      generatedId,
      GenerateESObject.genParentId(generatedId),
      Seq(CategoryES.fromGeneratedId(generatedId)),
      KeyValuePairES.fromFileTableList(fileTableList)
    )
  }

  def genGenerated(fileColumn: FileColumn, fileColumnList: List[(FileColumn, KeyValuePair)]): GeneratedES = {
    val generatedId = GenerateESObject.genGeneratedId("PostgreSQL", fileColumn.id)
    GeneratedES(
      FileColumn.entity,
      "PostgreSQL",
      generatedId,
      GenerateESObject.genParentId(generatedId),
      Seq(CategoryES.fromGeneratedId(generatedId)),
      KeyValuePairES.fromFileColumnList(fileColumnList)
    )
  }

  def genGenerated(sqlTable: SqlTable, sqlTableList: List[(SqlTable, KeyValuePair)]): GeneratedES = {
    val generatedId = GenerateESObject.genGeneratedId("PostgreSQL", sqlTable.id)
    GeneratedES(
      SqlTable.entity,
      "PostgreSQL",
      generatedId,
      GenerateESObject.genParentId(generatedId),
      Seq(CategoryES.fromGeneratedId(generatedId)),
      KeyValuePairES.fromSqlTableList(sqlTableList)
    )
  }

  def genGenerated(sqlColumn: SqlColumn, sqlColumnList: List[(SqlColumn, KeyValuePair)]): GeneratedES = {
    val generatedId = GenerateESObject.genGeneratedId("PostgreSQL", sqlColumn.id)
    GeneratedES(
      SqlColumn.entity,
      "PostgreSQL",
      generatedId,
      GenerateESObject.genParentId(generatedId),
      Seq(CategoryES.fromGeneratedId(generatedId)),
      KeyValuePairES.fromSqlColumnList(sqlColumnList)
    )
  }

}
