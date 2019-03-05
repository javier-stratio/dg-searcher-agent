package com.stratio.governance.agent.searcher.actors.manager

import org.scalatest.FlatSpec
import com.stratio.governance.agent.searcher.actors.manager.utils.defimpl.mustache.Mustache

class MustacheUnitTest extends FlatSpec {

  "Simple example " should " work" in {

    val template = new Mustache("Hello, {{ name }}!")
    val res: String = template.render(Map("name"->"world"))

    assertResult("Hello, world!")(res)
  }

  "List example " should " handle dynamic lists" in {

    val userTemplate = new Mustache("<strong>{{name}}</strong>")
    val baseTemplate = new Mustache(
      "<h2>Names</h2>{{#names}}{{> user}}{{/names}}"
    )
    val ctx = Map("names" -> List(
      Map("name" -> "Alice")
      , Map("name" -> "Bob")
    ))
    val partials = Map("user" -> userTemplate)
    val res: String = baseTemplate.render(ctx, partials)

    assertResult("<h2>Names</h2><strong>Alice</strong><strong>Bob</strong>")(res)
  }


}