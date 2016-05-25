package ch.unibas.dmi.dbis.adam.exception

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
case class EntityNotExistingException(message : String = "Entity not existing.")  extends GeneralAdamException(message)


