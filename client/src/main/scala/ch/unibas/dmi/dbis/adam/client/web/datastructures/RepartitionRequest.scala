package ch.unibas.dmi.dbis.adam.client.web.datastructures

/**
  * adampro
  *
  * Ivan Giangreco
  * April 2016
  */
case class RepartitionRequest(entity: String, partitions : Int, materialize : Boolean, replace : Boolean, usemetadata : Boolean, columns : Seq[String])

