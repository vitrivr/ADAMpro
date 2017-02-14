package org.vitrivr.adampro.web.datastructures

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2016
  */
private[web] object Entity {}

private[web] case class EntityListResponse(code: Int, entities: Seq[String])

private[web] case class EntityDetailResponse(code: Int, entityname: String, attribute : String, details: Map[String, String])

private[web] case class EntityCreateRequest(entityname: String, attributes: Seq[EntityField])

private[web] case class EntityField(name: String, datatype: String, storagehandler : String, params : Map[String, String])

private[web] case class EntityFillRequest(entityname: String, ntuples: Int, ndims: Int)

private[web] case class EntityImportRequest(host: String, database: String, username: String, password: String)

private[web] case class EntityReadResponse(code: Int, entityname: String, details: Seq[Map[String, String]])

private[web] case class EntityPartitionRequest(entityname: String, npartitions: Int, materialize: Boolean, replace: Boolean, attribute:String)

private[web] case class StorageHandlerResponse(code: Int, handlers : Map[String, Seq[String]])


