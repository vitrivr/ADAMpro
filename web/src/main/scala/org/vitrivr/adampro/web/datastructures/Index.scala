package org.vitrivr.adampro.web.datastructures

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2016
  */
private[web] object Index {}

private[web] case class IndexCreateRequest(entityname: String, attribute: String, norm: Int, indextype: String, options: Map[String, String])

private[web] case class IndexCreateAllRequest(entityname: String, attributes: Seq[EntityField])

private[web] case class IndexDetailResponse(code: Int, indexname: String, details: Map[String, String])

private[web] case class IndexPartitionRequest(indexname: String, npartitions: Int, materialize: Boolean, replace: Boolean, attribute: String)
