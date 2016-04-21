package ch.unibas.dmi.dbis.adam.client.web.datastructures

/**
  * adampro
  *
  * Ivan Giangreco
  * April 2016
  */
case class PreparationRequest(entityname: String, ntuples : Int, ndims : Int, fields : Seq[PreparationRequestField])
case class PreparationRequestField(name : String, datatype : String, indexed : Boolean)