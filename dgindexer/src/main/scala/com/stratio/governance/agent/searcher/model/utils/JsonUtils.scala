package com.stratio.governance.agent.searcher.model.utils

import org.json4s.native.JsonMethods.parse

object JsonUtils {

  def jsonStrToMap(jsonStr: String): Map[String, Any] = {
    implicit val formats = org.json4s.DefaultFormats

    parse(jsonStr).extract[Map[String, Any]]
  }
}
