package com.stratio.governance.agent.searcher.actors.indexer

import akka.actor.Actor
import com.stratio.governance.agent.searcher.actors.indexer.DGIndexer.IndexerEvent
import com.stratio.governance.agent.searcher.domain.SearchElementDomain.{BusinessAssetIndexerElement, DataAssetIndexerElement, QualityRuleIndexerElement}
import com.stratio.governance.agent.searcher.domain.SearchElementDomain
import com.stratio.governance.agent.searcher.actors.manager.DGManager
import com.stratio.governance.agent.searcher.model._
import com.stratio.governance.agent.searcher.model.es.ElasticObject
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

          // separate dataAsset information from additional (Business Assets and Quality Rules)
          val dataAssets: Array[ElasticObject] = chunkEnricher[DataAssetIndexerElement, String](chunk.filter( a => !params.getAdditionalBusiness.isAdditionalBusinessItem(a.tpe)),
            new DataAssetIndexerElement(params.getSourceDao)
          )

          val businessAssets: Array[ElasticObject] = chunkEnricher[BusinessAssetIndexerElement, Long](chunk.filter( a => a.tpe == params.getAdditionalBusiness.btType),
            new BusinessAssetIndexerElement(params.getSourceDao)
          )

          val qualityRules: Array[ElasticObject] = chunkEnricher[QualityRuleIndexerElement, Long](chunk.filter( a => a.tpe == params.getAdditionalBusiness.qrType),
            new QualityRuleIndexerElement(params.getSourceDao)
          )

          val list: Array[ElasticObject] = dataAssets ++ businessAssets ++ qualityRules

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

  def chunkEnricher[T,V](chunk: Array[ElasticObject], searchElement: T)(implicit indexerEnricher: SearchElementDomain.IndexerEnricher[T,V]): Array[ElasticObject] = {
    val batches: List[Array[ElasticObject]] = chunk.grouped(params.getPartition).toList

    val batchesEnriched: List[Array[ElasticObject]] = batches.map((x: Array[ElasticObject]) => {

      val additionals: Array[(Long, List[EntityRow])] = indexerEnricher.getAdditionals(x, searchElement)
      val ids_adds = additionals.map(a => a._1).toList

      val x_adds = x.filter(a => ids_adds.contains(SearchElementDomain.idtoLong(a.id)))
      val x_noAdds = x.filter(a => !ids_adds.contains(SearchElementDomain.idtoLong(a.id)))

      // x_adds management
      val ordered_x = x_adds.sortWith((a, b) => SearchElementDomain.idtoLong(a.id) < SearchElementDomain.idtoLong(b.id))
      val ordered_additionales = additionals.sortWith((a, b) => a._1 < b._1)
      val batchEnriched_adds: Array[ElasticObject] = for ((x, a) <- ordered_x zip ordered_additionales) yield {
        var maxTime: Long = x.getModifiedAt
        a._2.foreach {
          case BusinessAsset(_, name, _, _, tpe, modifiedAt) =>
            tpe match {
              case BusinessType.TERM => x.addBusinessTerm(name)
              case BusinessType.RULE => x.addBusinessRules(name)
            }
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
          case QualityRule(_, name, modifiedAt) =>
            x.addQualityRule(name)
            val upatedTs: Long = TimestampUtils.toLong(modifiedAt)
            if (upatedTs > maxTime) {
              maxTime = upatedTs
            }
        }
        x.modifiedAt = TimestampUtils.fromLong(maxTime)
        x
      }
      batchEnriched_adds ++ x_noAdds
    })

    batchesEnriched.fold[Array[ElasticObject]](Array())((a: Array[ElasticObject], b: Array[ElasticObject]) => {
      a ++ b
    })

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
  case class IndexerEvent(notificationChunks: Array[ElasticObject], token: Option[String])

}