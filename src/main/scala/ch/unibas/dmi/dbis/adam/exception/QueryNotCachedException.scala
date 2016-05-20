package ch.unibas.dmi.dbis.adam.exception

/**
  * adampro
  *
  * Ivan Giangreco
  * April 2016
  */
case class QueryNotCachedException()  extends GeneralAdamException("Query is not in cache.")
