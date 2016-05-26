package ch.unibas.dmi.dbis.adam.exception

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
case class IndexNotExistingException(message : String = "Index not existing.")  extends GeneralAdamException(message)
