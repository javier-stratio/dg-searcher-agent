package com.stratio.governance.agent.searcher.actors.manager.utils.defimpl

import com.stratio.governance.agent.searcher.actors.manager.dao.SourceDao
import com.stratio.governance.agent.searcher.actors.manager.scheduler.Scheduler
import com.stratio.governance.agent.searcher.actors.manager.utils.{ManagerUtils, ManagerUtilsException}
import com.vspy.mustache.Mustache
import org.slf4j.{Logger, LoggerFactory}

class DGManagerUtils(scheduler: Scheduler, sourceDao: SourceDao, relevance: List[String]) extends ManagerUtils {

  private lazy val LOG: Logger = LoggerFactory.getLogger(getClass.getName)

  val DOMAIN_TEMPLATE: String = "se-manager/governance_domain_template.json"
  val KEY_TEMPLATE_1: String = "se-manager/governance_key_template_1.json"
  val KEY_TEMPLATE_2: String = "se-manager/governance_key_template_2.json"

  @throws(classOf[ManagerUtilsException])
  override def getGeneratedModel(): String = {
    val domain_template: String = loadResource(DOMAIN_TEMPLATE)
      .replace(RelevanceRef.RELEVANCE_ALIAS_REF._2, relevance(RelevanceRef.RELEVANCE_ALIAS_REF._3))
      .replace(RelevanceRef.RELEVANCE_NAME_REF._2, relevance(RelevanceRef.RELEVANCE_NAME_REF._3))
      .replace(RelevanceRef.RELEVANCE_DESCRIPTION_REF._2, relevance(RelevanceRef.RELEVANCE_DESCRIPTION_REF._3))
      .replace(RelevanceRef.RELEVANCE_BUSINESSTERM_REF._2, relevance(RelevanceRef.RELEVANCE_BUSINESSTERM_REF._3))
      .replace(RelevanceRef.RELEVANCE_KEY_REF._2, relevance(RelevanceRef.RELEVANCE_KEY_REF._3))
    val key_template_1: String = loadResource(KEY_TEMPLATE_1)
    val key_template_2: String = loadResource(KEY_TEMPLATE_2)
      .replace(RelevanceRef.RELEVANCE_VALUE_REF._2, relevance(RelevanceRef.RELEVANCE_VALUE_REF._3))

    val keys: List[String] = sourceDao.getKeys()

    val domainTemplate = new Mustache(domain_template)
    val keyTemplate1 = new Mustache(key_template_1)
    val keyTemplate2 = new Mustache(key_template_2)

    val ctx = Map("keyFields" -> keys.map( k => {
      Map("field" -> k,"name" -> k)
    }), "keySearchs" -> keys.map( k => {
      Map("field" -> k)
    }) )
    val partials = Map("keyField" -> keyTemplate1, "keySearch" -> keyTemplate2)
    val res: String = domainTemplate.render(ctx, partials).replace("\n","")
    res
  }

  @throws(classOf[ManagerUtilsException])
  override def getScheduler(): Scheduler = {
    scheduler
  }

  private def loadResource(filename: String): String = {
    val source = scala.io.Source.fromInputStream(getClass().getClassLoader().getResourceAsStream(filename))
    try source.mkString finally source.close()
  }

}

object RelevanceRef {

  val RELEVANCE_ALIAS_REF: (String, String, Int)= ("manager.relevance.alias","RELEVANCE_ALIAS",0)
  val RELEVANCE_NAME_REF: (String, String, Int)= ("manager.relevance.name","RELEVANCE_NAME",1)
  val RELEVANCE_DESCRIPTION_REF: (String, String, Int)= ("manager.relevance.description","RELEVANCE_DESCRIPTION",2)
  val RELEVANCE_BUSINESSTERM_REF: (String, String, Int)= ("manager.relevance.businessterm","RELEVANCE_BUSINESSTERM",3)
  val RELEVANCE_KEY_REF: (String, String, Int)= ("manager.relevance.key","RELEVANCE_KEY",4)
  val RELEVANCE_VALUE_REF: (String, String, Int)= ("manager.relevance.value","RELEVANCE_VALUE",5)

}