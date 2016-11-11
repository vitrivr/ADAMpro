package org.vitrivr.adampro.exception

/**
  * adampro
  *
  * Ivan Giangreco
  * March 2016
  */
case class EntityNotProperlyDefinedException(message : String = "Entity not properly defined.")  extends GeneralAdamException(message)
