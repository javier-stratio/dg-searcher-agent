package com.stratio.governance.agent.searcher.actors.indexer

import akka.actor.Actor
import com.stratio.governance.agent.searcher.actors.indexer.DGIndexer.IndexerEvent
import com.stratio.governance.agent.searcher.model.{BusinessTerm, EntityRow, KeyValuePair}
import com.stratio.governance.agent.searcher.model.es.{DataAssetES, EntityRowES}
import com.stratio.governance.commons.agent.domain.dao.DataAssetDao
import org.json4s.DefaultFormats
import org.json4s.JsonAST.{JArray, JObject}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class DGIndexer(params: IndexerParams) extends Actor {

  override def receive: Receive = {
    // The whole chunk will be send to Indexer. It must come previously partitioned
    case IndexerEvent(chunk) =>

      val batches: List[Array[DataAssetDao]] = chunk.grouped(params.getPartiton()).toList

      val batchesEnriched: List[Array[DataAssetES]] = batches.map( (x: Array[DataAssetDao]) => {

        val ids: Array[Int] = x.map( (dadao: DataAssetDao) => dadao.id )

        val functionList: List[ (Array[Int]) => List[EntityRow] ] = List(params.getSourceDao().keyValuePairProcess,params.getSourceDao().businessTerms)

        val relatedInfo: List[List[EntityRow]] = functionList.par.map(f => {
          val erES = f(ids)
          erES
        }).toList

        val additionals: Array[(Long,List[EntityRow])] = pivotRelatedInfo(relatedInfo)
        val ids_adds = additionals.map(a => a._1).toList

        val x_adds = x.filter( a => ids_adds.contains(a.id))
        val x_noAdds = x.filter( a => !ids_adds.contains(a.id))

        // x_adds management
        val ordered_x = x_adds.sortWith((a, b) => (a.id < b.id))
        val ordered_additionales = additionals.sortWith((a, b) => (a._1 < b._1))
        val batchEnriched_adds: Array[DataAssetES] = for ( (x, a) <- (ordered_x zip ordered_additionales)) yield {
          var daes: DataAssetES = DataAssetES.fromDataAssetDao(x)
          var maxTime: Long = daes.getModifiedAt()
          a._2.foreach( add => { add match {
              case BusinessTerm(id,term,updated_at) => {
                daes.addBusinessTerm(term)
                val upatedTs: Long = DataAssetES.dateStrToTimestamp(updated_at)
                if (upatedTs > maxTime) {
                  maxTime = upatedTs
                }
              }
              case KeyValuePair(_,key,value,updated_at) => {
                daes.addKeyValue(key,value)
                val upatedTs: Long = DataAssetES.dateStrToTimestamp(updated_at)
                if (upatedTs > maxTime) {
                  maxTime = upatedTs
                }
              }
            }
          })
          daes.setModifiedAt(DataAssetES.timestampTodateStr(maxTime))
          daes
        }

        // x_noAdds management
        val batchEnriched_noAdds: Array[DataAssetES] = x_noAdds.map( x => DataAssetES.fromDataAssetDao(x))

        (batchEnriched_adds ++ batchEnriched_noAdds)
      })

      val list: Array[DataAssetES] = batchesEnriched.fold[Array[DataAssetES]](Array())((a: Array[DataAssetES], b: Array[DataAssetES]) => {a ++ b})
      val jlist: JArray = JArray(list.map( a => a.getJsonObject()).toList)

      implicit val formats = DefaultFormats
      val documentsBulk: String = org.json4s.native.Serialization.write(jlist)

      sender ! Future(params.getSearcherDao().index(documentsBulk))

  }

  def pivotRelatedInfo(info: List[List[EntityRow]]): Array[(Long,List[EntityRow])] = {

    val list_completed: List[EntityRow] = info.fold[List[EntityRow]](List())( (a,b) => { a ++ b } )
    val list_completed_by_id: Map[Long,List[EntityRow]] =  list_completed.map( e => (e.getId(),e) ).groupBy[Long]( a => a._1 ).mapValues( (l: List[(Long,EntityRow)]) => {
      l.map( a => a._2 )
    })

  return list_completed_by_id.toArray[(Long, List[EntityRow])]
  }

}


object DGIndexer {

  /**
    * Default Actor name
    */
  lazy val NAME = "Partial Indexer"

  /**
    * Actor messages
    */
  case class IndexerEvent(notificationChunks: Array[DataAssetDao])

}
