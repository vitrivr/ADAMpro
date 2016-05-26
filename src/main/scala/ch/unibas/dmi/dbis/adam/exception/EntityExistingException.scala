package ch.unibas.dmi.dbis.adam.exception

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
case class EntityExistingException(message : String = "Entity exists already.")  extends GeneralAdamException(message)
