package ch.unibas.dmi.dbis.adam.client.web.datastructures

/**
  * adampro
  *
  * Ivan Giangreco
  * April 2016
  */
case class IndexRequest(entityname: String, norm : Int, indextype : String, options : Map[String, String])

