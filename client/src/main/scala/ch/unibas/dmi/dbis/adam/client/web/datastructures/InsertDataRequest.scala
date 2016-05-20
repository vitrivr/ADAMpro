package ch.unibas.dmi.dbis.adam.client.web.datastructures

/**
  * adampro
  *
  * Ivan Giangreco
  * May 2016
  */
case class InsertDataRequest(entityname: String, ntuples : Int, ndims : Int, fields : Seq[EntityField])