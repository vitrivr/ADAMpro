package ch.unibas.dmi.dbis.adam.client.web.datastructures

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2016
  */
object Entity {}

case class EntityListResponse(code: Int, entities: Seq[String])

case class EntityDetailResponse(code: Int, entityname: String, details: Map[String, String])

case class EntityCreateRequest(entityname: String, attributes: Seq[EntityField])

case class EntityField(name: String, datatype: String, indexed: Boolean, pk: Boolean)

case class EntityFillRequest(entityname: String, ntuples: Int, ndims: Int)

case class EntityImportRequest(host: String, database: String, username: String, password: String)

case class EntityReadResponse(code: Int, entityname: String, details: Seq[Map[String, String]])

case class EntityPartitionRequest(entityname: String, npartitions: Int, materialize: Boolean, replace: Boolean, attributes: Seq[String])

