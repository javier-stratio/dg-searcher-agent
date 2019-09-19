package com.stratio.governance.agent.searcher.domain

import com.stratio.governance.agent.searcher.actors.dao.postgres.PartialIndexationFields
import com.stratio.governance.agent.searcher.actors.indexer.dao.SourceDao
import com.stratio.governance.agent.searcher.model.EntityRow
import com.stratio.governance.agent.searcher.model.es.ElasticObject

object SearchElementDomain {

  val IDS_CONDITION_PLACEHOLDER: String = "{{idscond}}"

  trait IndexerElement // T class
  class DataAssetIndexerElement(val sourceDao: SourceDao) extends IndexerElement
  class BusinessAssetIndexerElement(val sourceDao: SourceDao) extends IndexerElement
  class QualityRuleIndexerElement(val sourceDao: SourceDao) extends IndexerElement

  // T Type Class interfaces
  trait IndexerEnricher[T, V] {
    def getFunctions(sourceDao: SourceDao): List[List[V] => List[EntityRow]]
    def getAdditionals(elements: Array[ElasticObject], t: T): Array[(Long, List[EntityRow])]
  }

  trait ReaderElement // W class
  class DataAssetReaderElement(val schema: String, val dataAsset: String, val keyDataAsset: String, val key: String, val businessAssetDataAsset: String, val businessAsset: String, val quality: String) extends ReaderElement
  class BusinessAssetReaderElement(val schema: String, val businessAssets: String, val businessAssetType: String, val businessAssetsStatus: String, val keyBusinessAssets: String, val key: String) extends ReaderElement
  class QualityRuleReaderElement(val schema: String, val quality: String, val keyQuality: String, val key: String, val businessAssetsQuality: String, val businessAssets: String) extends ReaderElement

  // W Type Class interfaces
  trait Reader[W] {
    def getTotalIndexationQueryInfo(w: W, types: List[String]): String
    def getPartialIndexationQueryInfo(w: W, resultNumber: Int): (String, List[(Int, String)])
  }

  object IndexerEnricher extends IndexerEnricherInstances
  object Reader extends ReaderInstances

  // T Type Class instances
  trait IndexerEnricherInstances {
    def apply[T, V](implicit ev: IndexerEnricher[T, V]) = ev

    implicit def DataAssetIndexerEnricherInstance = new IndexerEnricher[DataAssetIndexerElement, String] {

      def getFunctions(sourceDao: SourceDao): List[List[String] => List[EntityRow]] = {
        List( sourceDao.keyValuePairForDataAsset, sourceDao.businessTermsForDataAsset, sourceDao.qualityRulesForDataAsset )
      }

      def getAdditionals(elements: Array[ElasticObject], t: DataAssetIndexerElement): Array[(Long, List[EntityRow])] = {
        val ids: Map[String, Long] = elements.map( (dadao: ElasticObject) => (dadao.metadataPath, idtoLong( dadao.id )) ).toMap

        val relatedInfo: List[List[EntityRow]] = getFunctions( t.sourceDao ).par.map( f => {
          val erES = f( ids.keySet.toList )
          erES
        } ).toList

        // Pivot information
        val list_completed: List[EntityRow] = relatedInfo.fold[List[EntityRow]]( List() )( (a, b) => {
          a ++ b
        } )
        val list_completed_by_id: Map[Long, List[EntityRow]] = list_completed.map( e => (ids.get( e.getIdentifier ).get, e) ).groupBy[Long]( a => a._1 ).mapValues( (l: List[(Long, EntityRow)]) => {
          l.map( a => a._2 )
        } )

        list_completed_by_id.toArray[(Long, List[EntityRow])]
      }
    }

    implicit def BusinessAssetIndexerEnricherInstance = new IndexerEnricher[BusinessAssetIndexerElement, Long] {

      def getFunctions(sourceDao: SourceDao): List[List[Long] => List[EntityRow]] = {
        List( sourceDao.keyValuePairForBusinessAsset )
      }

      def getAdditionals(elements: Array[ElasticObject], t: BusinessAssetIndexerElement): Array[(Long, List[EntityRow])] = {
        getAdditionalsForAddElements( elements, getFunctions( t.sourceDao ) )
      }
    }

    implicit def QualityRuleSearchElement = new IndexerEnricher[QualityRuleIndexerElement, Long] {

      def getFunctions(sourceDao: SourceDao): List[List[Long] => List[EntityRow]] = {
        List( sourceDao.keyValuePairForQualityRule, sourceDao.businessRulesForQualityRule )
      }

      def getAdditionals(elements: Array[ElasticObject], t: QualityRuleIndexerElement): Array[(Long, List[EntityRow])] = {
        getAdditionalsForAddElements( elements, getFunctions( t.sourceDao ) )
      }
    }

    def getAdditionalsForAddElements(elements: Array[ElasticObject], functionList: List[List[Long] => List[EntityRow]]): Array[(Long, List[EntityRow])] = {
      val ids: List[Long] = elements.map( (eo: ElasticObject) => idtoLong( eo.id ) ).toList

      val relatedInfo: List[List[EntityRow]] = functionList.par.map( f => {
        val erES = f( ids )
        erES
      } ).toList

      // Pivot information
      val list_completed: List[EntityRow] = relatedInfo.fold[List[EntityRow]]( List() )( (a, b) => {
        a ++ b
      } )
      val list_completed_by_id: Map[Long, List[EntityRow]] = list_completed.groupBy[Long]( a => a.getIdentifier.toLong )

      list_completed_by_id.toArray[(Long, List[EntityRow])]
    }
  }

  def idtoLong(identifier: String): Long = {
    identifier.split( "/" ).last.toLong
  }

  // W Type Class instances
  trait ReaderInstances {
    def apply[W](implicit ev: Reader[W]) = ev

    implicit def DataAssetReaderInstance = new Reader[DataAssetReaderElement] {

      def getTotalIndexationQueryInfo(w: DataAssetReaderElement, types: List[String]): String = {
        s"SELECT id,name,alias,description,metadata_path,type,subtype,tenant,properties,active,discovered_at,modified_at FROM ${w.schema}.${w.dataAsset} WHERE active = true"
      }

      def getPartialIndexationQueryInfo(w: DataAssetReaderElement, resultNumber: Int): (String, List[(Int, String)]) = {
        (s" SELECT metadata_path, 0, modified_at,${resultNumber} FROM ${w.schema}.${w.dataAsset} WHERE modified_at > ? " +
          "UNION " +
          s"SELECT metadata_path, 0, modified_at,${resultNumber+1} FROM ${w.schema}.${w.keyDataAsset} WHERE modified_at > ? " +
          "UNION " +
          s"SELECT key_data_asset.metadata_path, 0, key.modified_at,${resultNumber+2} FROM ${w.schema}.${w.keyDataAsset} AS key_data_asset, " +
          s"${w.schema}.${w.key} AS key WHERE key.id = key_data_asset.key_id and key.modified_at > ? " +
          "UNION " +
          s"SELECT metadata_path, 0, modified_at,${resultNumber+3} FROM ${w.schema}.${w.businessAssetDataAsset} WHERE modified_at > ? " +
          "UNION " +
          s"SELECT bus_assets_data_assets.metadata_path, 0, bus_assets.modified_at,${resultNumber+4} FROM ${w.schema}.${w.businessAssetDataAsset} AS bus_assets_data_assets, " +
          s"${w.schema}.${w.businessAsset} AS bus_assets WHERE bus_assets_data_assets.business_assets_id = bus_assets.id and bus_assets.modified_at > ? " +
          "UNION " +
          s"SELECT metadata_path, 0, modified_at,${resultNumber+5} FROM ${w.schema}.${w.quality} WHERE modified_at > ? ",
          List[(Int, String)](
            (1,PartialIndexationFields.DATA_ASSET),
            (2,PartialIndexationFields.KEY_DATA_ASSET),
            (3,PartialIndexationFields.KEY),
            (4,PartialIndexationFields.BUSINESS_ASSET_DATA_ASSET),
            (5,PartialIndexationFields.BUSINESS_ASSET),
            (6,PartialIndexationFields.QUALITY_RULE)
          ))
      }

    }

    implicit def BusinessAssetReaderInstance = new Reader[BusinessAssetReaderElement] {

      def getTotalIndexationQueryInfo(w: BusinessAssetReaderElement, types: List[String]): String = {
        s"select ba.id as id,ba.name as name,'' as alias,ba.description as description,'' as metadata_path,'${types(0)}' as type,bat.description as subtype,ba.tenant,null as properties,true as active,ba.modified_at as discovered_at,ba.modified_at as modified_at from ${w.schema}.${w.businessAssets} as ba, ${w.schema}.${w.businessAssetType} as bat, ${w.schema}.${w.businessAssetsStatus} as bas where ba.business_assets_type_id = bat.id and ba.business_assets_status_id = bas.id and bas.active = true"
      }

      def getPartialIndexationQueryInfo(w: BusinessAssetReaderElement, resultNumber: Int): (String, List[(Int, String)]) = {
          (s"SELECT '', ba.id, ba.modified_at, $resultNumber FROM ${w.schema}.${w.businessAssets} as ba, ${w.schema}.${w.businessAssetsStatus} as bas WHERE ba.business_assets_status_id = bas.id and bas.active = true and ba.modified_at > ? " +
          s"UNION " +
          s"SELECT '', key_business_assets.business_assets_id, modified_at, ${resultNumber + 1} FROM ${w.schema}.${w.keyBusinessAssets} as key_business_assets WHERE modified_at > ? " +
          s"UNION " +
          s"SELECT '', key_business_assets.business_assets_id, key.modified_at, ${resultNumber + 2} FROM ${w.schema}.${w.keyBusinessAssets} AS key_business_assets, " +
          s"${w.schema}.${w.key} AS key WHERE key.id = key_business_assets.key_id and key.modified_at > ?",
          List[(Int, String)](
            (resultNumber, PartialIndexationFields.BUSINESS_ASSET),
            (resultNumber+1, PartialIndexationFields.KEY_BUSINESS_ASSET),
            (resultNumber+2, PartialIndexationFields.KEY)
          )
          )
      }

    }

    implicit def QualityRuleReaderInstance = new Reader[QualityRuleReaderElement] {

      def getTotalIndexationQueryInfo(w: QualityRuleReaderElement, types: List[String]): String = {
        s"(select id,name,'' as alias,description,'' as metadata_path,'${types(0)}' as type,'${types(1)}' as subtype, tenant,null as properties, active, modified_at as discovered_at, modified_at from ${w.schema}.${w.quality} where metadata_path <> '' and quality_generic_id is null " + IDS_CONDITION_PLACEHOLDER + ") UNION " +
        s"(select id,name,'' as alias,description,'' as metadata_path,'${types(0)}' as type,'${types(2)}' as subtype, tenant,null as properties, active, modified_at as discovered_at, modified_at from ${w.schema}.${w.quality} where metadata_path = '' " + IDS_CONDITION_PLACEHOLDER + ")"
      }

      def getPartialIndexationQueryInfo(w: QualityRuleReaderElement, resultNumber: Int): (String, List[(Int, String)]) = {
        (s"SELECT '', id, modified_at, $resultNumber FROM ${w.schema}.${w.quality} WHERE modified_at > ? " +
          s"UNION " +
          s"SELECT '', key_quality.quality_id, modified_at, ${resultNumber + 1} FROM ${w.schema}.${w.keyQuality} as key_quality WHERE modified_at > ? " +
          s"UNION " +
          s"SELECT '', key_quality.quality_id, key.modified_at, ${resultNumber + 2} FROM ${w.schema}.${w.keyQuality} AS key_quality, " +
          s"${w.schema}.${w.key} AS key WHERE key.id = key_quality.key_id and key.modified_at > ? " +
          s"UNION " +
          s"SELECT '', business_assets_quality.quality_id, modified_at, ${resultNumber + 3} FROM ${w.schema}.${w.businessAssetsQuality} as business_assets_quality WHERE modified_at > ? " +
          s"UNION " +
          s"SELECT '', business_assets_quality.quality_id, business_assets.modified_at, ${resultNumber + 4} FROM ${w.schema}.${w.businessAssetsQuality} AS business_assets_quality, " +
          s"${w.schema}.${w.businessAssets} AS business_assets WHERE business_assets.id = business_assets_quality.business_assets_id and business_assets.modified_at > ?",
          List[(Int, String)](
            (resultNumber,PartialIndexationFields.QUALITY_RULE),
            (resultNumber+1,PartialIndexationFields.KEY_QUALITY),
            (resultNumber+2,PartialIndexationFields.KEY),
            (resultNumber+3,PartialIndexationFields.BUSINESS_ASSET_QUALITY),
            (resultNumber+4,PartialIndexationFields.BUSINESS_ASSET)
          )
        )
      }
    }
  }

}
