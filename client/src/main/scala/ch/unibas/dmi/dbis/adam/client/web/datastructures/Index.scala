package ch.unibas.dmi.dbis.adam.client.web.datastructures

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2016
  */
private[client] object Index {}

private[client] case class IndexCreateRequest(entityname: String, attribute: String, norm: Int, indextype: String, options: Map[String, String])

private[client] case class IndexCreateAllRequest(entityname: String, attributes: Seq[EntityField])

private[client] case class IndexPartitionRequest(indexname: String, npartitions: Int, materialize: Boolean, replace: Boolean, attributes: Seq[String])
