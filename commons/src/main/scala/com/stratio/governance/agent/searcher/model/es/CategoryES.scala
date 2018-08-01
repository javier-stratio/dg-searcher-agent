package com.stratio.governance.agent.searcher.model.es

case class CategoryES(id: String, name: String)

object CategoryES {

  //indexer component requests thr Category as an array of arrays. This is why a Seq[CategoryES] is returned.
  //TODO instead of that, CategoryES should be a Seq of id & name elements
  def fromGeneratedId(generatedId: String): Seq[CategoryES] = {
    val x = Seq(CategoryES(generatedId.split("/").take(1).mkString("/"), generatedId.split("/").take(1).mkString("/")))
    x
  }

}
