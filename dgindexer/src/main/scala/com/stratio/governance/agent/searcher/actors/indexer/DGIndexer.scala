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

class DGIndexer(params: IndexerParams) extends Actor {

  private lazy val LOG: Logger = LoggerFactory.getLogger(getClass.getName)
  implicit val formats: DefaultFormats.type = DefaultFormats

  override def receive: Receive = {
      // The whole chunk will be send to Indexer. It must come previously partitioned
      case IndexerEvent(chunk, token) =>
        try {
          LOG.debug("handling Indexer Event. token: " + token)

          // first at all separate dataAsset information from additional, this last one will not be process here
          val realda: Array[DataAssetES] = chunk.filter(a => !params.getAdditionalBusiness.isAdaptable(a.tpe))
          val additionalda: Array[DataAssetES] = chunk.filter(a => params.getAdditionalBusiness.isAdaptable(a.tpe))

          val batches: List[Array[DataAssetES]] = realda.grouped(params.getPartition).toList

          val batchesEnriched: List[Array[DataAssetES]] = batches.map((x: Array[DataAssetES]) => {

            val ids: Map[String, Long] = x.filter( dadao => !params.getAdditionalBusiness.isAdaptable(dadao.tpe)).map((dadao: DataAssetES) => (dadao.metadataPath, dataAssetIdtoLong(dadao.id))).toMap

            val functionList: List[List[String] => List[EntityRow]] = List(params.getSourceDao.keyValuePairProcess, params.getSourceDao.businessAssets)

            val relatedInfo: List[List[EntityRow]] = functionList.par.map(f => {
              val erES = f(ids.keySet.toList)
              erES
            }).toList

            val additionals: Array[(Long, List[EntityRow])] = pivotRelatedInfo(relatedInfo, ids)
            val ids_adds = additionals.map(a => a._1).toList

            val x_adds = x.filter(a => ids_adds.contains(dataAssetIdtoLong(a.id)))
            val x_noAdds = x.filter(a => !ids_adds.contains(dataAssetIdtoLong(a.id)))

            // x_adds management
            val ordered_x = x_adds.sortWith((a, b) => dataAssetIdtoLong(a.id) < dataAssetIdtoLong(b.id))
            val ordered_additionales = additionals.sortWith((a, b) => a._1 < b._1)
            val batchEnriched_adds: Array[DataAssetES] = for ((x, a) <- ordered_x zip ordered_additionales) yield {
              var maxTime: Long = x.getModifiedAt
              a._2.foreach {
                case BusinessAsset(_, name, _, _, _, modifiedAt) =>
                  x.addBusinessTerm(name)
                  val upatedTs: Long = TimestampUtils.toLong(modifiedAt)
                  if (upatedTs > maxTime) {
                    maxTime = upatedTs
                  }
                case KeyValuePair(_, key, value, updated_at) =>
                  x.addKeyValue(key, value)
                  val upatedTs: Long = TimestampUtils.toLong(updated_at)
                  if (upatedTs > maxTime) {
                    maxTime = upatedTs
                  }
              }
              x.modifiedAt = TimestampUtils.fromLong(maxTime)
              x
            }
            batchEnriched_adds ++ x_noAdds
          })

          val list: Array[DataAssetES] = batchesEnriched.fold[Array[DataAssetES]](Array())((a: Array[DataAssetES], b: Array[DataAssetES]) => {
            a ++ b
          }) ++ additionalda

          if (!list.isEmpty) {
            val jlist: JArray = JArray( list.map( a => a.getJsonObject ).toList )

            val documentsBulk: String = org.json4s.native.Serialization.write( jlist )

            token match {
              case Some( tk ) =>
                sender ! params.getSearcherDao.indexTotal( DGManager.MODEL_NAME, documentsBulk, tk )
              case None =>
                sender ! params.getSearcherDao.indexPartial( DGManager.MODEL_NAME, documentsBulk )
            }
          } else {
            LOG.debug("No documents to index")
            sender ! "Nothing to send"
          }
        } catch {
          case e: Throwable =>
            LOG.error("Error while processing", e)
        }

  }

  def pivotRelatedInfo(info: List[List[EntityRow]], idsMap: Map[String, Long]): Array[(Long,List[EntityRow])] = {

    val list_completed: List[EntityRow] = info.fold[List[EntityRow]](List())( (a,b) => { a ++ b } )
    val list_completed_by_id: Map[Long,List[EntityRow]] =  list_completed.map( e => (idsMap.get(e.getMatadataPath).get,e) ).groupBy[Long]( a => a._1 ).mapValues( (l: List[(Long,EntityRow)]) => {
      l.map( a => a._2 )
    })

    list_completed_by_id.toArray[(Long, List[EntityRow])]
  }

  private def dataAssetIdtoLong(identifier: String): Long = {
    identifier.split("/").last.toLong
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