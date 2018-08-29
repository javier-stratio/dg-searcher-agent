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

  def genGeneratedFromKeyValuePair(entity: EntityRow, keyValuePair: KeyValuePair): GeneratedES = {
    val generatedId = GenerateESObject.genGeneratedId("PostgreSQL", entity.id)
    GeneratedES(
      DatastoreEngine.entity,
      "PostgreSQL",
      generatedId,
      GenerateESObject.genParentId(generatedId),
      Seq(CategoryES.fromGeneratedId(generatedId)),
      KeyValuePairES.fromKeyValuePair(keyValuePair)
    )
  }


  def genGeneratedFromList(databaseSchema: EntityRow,
                   databaseSchemaList: List[(EntityRow, KeyValuePair)]): GeneratedES = {
    val generatedId = GenerateESObject.genGeneratedId("PostgreSQL", databaseSchema.id)
    GeneratedES(
      DatabaseSchema.entity,
      "PostgreSQL",
      generatedId,
      GenerateESObject.genParentId(generatedId),
      Seq(CategoryES.fromGeneratedId(generatedId)),
      KeyValuePairES.fromList(databaseSchemaList)
    )
  }

}
