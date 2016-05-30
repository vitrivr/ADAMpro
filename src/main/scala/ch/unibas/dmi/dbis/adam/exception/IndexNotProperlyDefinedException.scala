package ch.unibas.dmi.dbis.adam.exception

/**
  * adamtwo
  *
  * Ivan Giangreco
  * May 2016
  */
case class IndexNotProperlyDefinedException(message : String = "Index not properly defined.")  extends GeneralAdamException(message)

