package org.vitrivr.adampro.utils.exception

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
case class IndexExistingException(message : String = "Index exists already.")  extends GeneralAdamException(message)