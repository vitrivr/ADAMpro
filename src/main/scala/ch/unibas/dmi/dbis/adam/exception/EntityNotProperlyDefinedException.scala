package ch.unibas.dmi.dbis.adam.exception

/**
  * adampro
  *
  * Ivan Giangreco
  * March 2016
  */
case class EntityNotProperlyDefinedException(details : Option[String] = None)  extends GeneralAdamException(details.getOrElse("Entity not properly defined."))
