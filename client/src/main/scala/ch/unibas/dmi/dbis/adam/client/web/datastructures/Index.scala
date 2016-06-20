package ch.unibas.dmi.dbis.adam.client.web.datastructures

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2016
  */
object Index {}

case class IndexCreateRequest(entityname: String, attribute: String, norm: Int, indextype: String, options: Map[String, String])

case class IndexCreateAllRequest(entityname: String, attributes: Seq[EntityField])

case class IndexPartitionRequest(indexname: String, npartitions: Int, materialize: Boolean, replace: Boolean, attributes: Seq[String])
