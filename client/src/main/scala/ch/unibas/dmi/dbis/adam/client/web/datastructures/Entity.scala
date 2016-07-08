package ch.unibas.dmi.dbis.adam.client.web.datastructures

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2016
  */
private[client] object Entity {}

private[client] case class EntityListResponse(code: Int, entities: Seq[String])

private[client] case class EntityDetailResponse(code: Int, entityname: String, details: Map[String, String])

private[client] case class EntityCreateRequest(entityname: String, attributes: Seq[EntityField])

private[client] case class EntityField(name: String, datatype: String, indexed: Boolean, pk: Boolean)

private[client] case class EntityFillRequest(entityname: String, ntuples: Int, ndims: Int)

private[client] case class EntityImportRequest(host: String, database: String, username: String, password: String)

private[client] case class EntityReadResponse(code: Int, entityname: String, details: Seq[Map[String, String]])

private[client] case class EntityPartitionRequest(entityname: String, npartitions: Int, materialize: Boolean, replace: Boolean, attributes: Seq[String])

