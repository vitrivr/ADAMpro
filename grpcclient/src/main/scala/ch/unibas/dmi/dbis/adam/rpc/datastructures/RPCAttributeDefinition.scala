package ch.unibas.dmi.dbis.adam.rpc.datastructures

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * July 2016
  */
case class RPCAttributeDefinition(name: String, datatype: String, pk: Boolean = false, storagehandlername : Option[String] = None, params : Map[String, String] = Map())

