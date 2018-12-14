package com.stratio.governance.agent.searcher.actors.indexer

import akka.actor.Actor
import com.stratio.governance.agent.searcher.actors.indexer.DGIndexer.IndexerEvent
import com.stratio.governance.agent.searcher.actors.manager.DGManager
import com.stratio.governance.agent.searcher.model._
import com.stratio.governance.agent.searcher.model.es.DataAssetES
import com.stratio.governance.agent.searcher.model.utils.TimestampUtils
import org.json4s.DefaultFormats
import org.json4s.JsonAST.JArray
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class DGIndexer(params: IndexerParams) extends Actor {

  private lazy val LOG: Logger = LoggerFactory.getLogger(getClass.getName)
  implicit val formats: DefaultFormats.type = DefaultFormats

  override def receive: Receive = {
      // The whole chunk will be send to Indexer. It must come previously partitioned
      case IndexerEvent(chunk, token) =>
        try {
          LOG.debug("handling Indexer Event. token: " + token)
          val batches: List[Array[DataAssetES]] = chunk.grouped(params.getPartition).toList

          val batchesEnriched: List[Array[DataAssetES]] = batches.map((x: Array[DataAssetES]) => {

            val ids: Array[Int] = x.map((dadao: DataAssetES) => dadao.id)

            val functionList: List[Array[Int] => List[EntityRow]] = List(params.getSourceDao.keyValuePairProcess, params.getSourceDao.businessAssets)

            val relatedInfo: List[List[EntityRow]] = functionList.par.map(f => {
              val erES = f(ids)
              erES
            }).toList

            val additionals: Array[(Long, List[EntityRow])] = pivotRelatedInfo(relatedInfo)
            val ids_adds = additionals.map(a => a._1).toList

            val x_adds = x.filter(a => ids_adds.contains(a.id))
            val x_noAdds = x.filter(a => !ids_adds.contains(a.id))

            // x_adds management
            val ordered_x = x_adds.sortWith((a, b) => a.id < b.id)
            val ordered_additionales = additionals.sortWith((a, b) => a._1 < b._1)
            val batchEnriched_adds: Array[DataAssetES] = for ((x, a) <- ordered_x zip ordered_additionales) yield {
              var maxTime: Long = x.getModifiedAt
              a._2.foreach {
                case BusinessAsset(id, name, description, status, tpe, modifiedAt) =>
                  x.addBusinessTerm(name)
                  val upatedTs: Long = TimestampUtils.toLong(modifiedAt)
                  if (upatedTs > maxTime) {
                    maxTime = upatedTs
                  }
                case KeyValuePair(id, key, value, updated_at) =>
                  x.addKeyValue(key, value)
                  val upatedTs: Long = TimestampUtils.toLong(updated_at)
                  if (upatedTs > maxTime) {
                    maxTime = upatedTs
                  }
              }
              x.setModifiedAt(TimestampUtils.fromLong(maxTime))
              x
            }
            batchEnriched_adds ++ x_noAdds
          })

          val list: Array[DataAssetES] = batchesEnriched.fold[Array[DataAssetES]](Array())((a: Array[DataAssetES], b: Array[DataAssetES]) => {
            a ++ b
          })
          val jlist: JArray = JArray(list.map(a => a.getJsonObject).toList)

          val documentsBulk: String = org.json4s.native.Serialization.write(jlist)

          token match {
            case Some(tk) =>
              sender ! Future(params.getSearcherDao.indexTotal(DGManager.MODEL_NAME, documentsBulk, tk))
            case None =>
              sender ! Future(params.getSearcherDao.indexPartial(DGManager.MODEL_NAME, documentsBulk))
          }
        } catch {
          case e: Throwable =>
            LOG.error("Error while processing", e)
        }

  }

  def pivotRelatedInfo(info: List[List[EntityRow]]): Array[(Long,List[EntityRow])] = {

    val list_completed: List[EntityRow] = info.fold[List[EntityRow]](List())( (a,b) => { a ++ b } )
    val list_completed_by_id: Map[Long,List[EntityRow]] =  list_completed.map( e => (e.getId,e) ).groupBy[Long]( a => a._1 ).mapValues( (l: List[(Int,EntityRow)]) => {
      l.map( a => a._2 )
    })

    list_completed_by_id.toArray[(Long, List[EntityRow])]
  }
}


object DGIndexer {

  /**
    * Default Actor name
    */
  lazy val NAME = "DGIndexer"

  /**
    * Actor messages
    */
  case class IndexerEvent(notificationChunks: Array[DataAssetES], token: Option[String])

}