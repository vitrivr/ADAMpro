package ch.unibas.dmi.dbis.adam.exception

/**
  * adampro
  *
  * Ivan Giangreco
  * March 2016
  */
case class NoMetadataAvailableException  extends GeneralAdamException("No metadata available, but requesting.")
//TODO: possibly replace by warning