package ch.unibas.dmi.dbis.adam.exception

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
class AdamTwoException  private(ex: RuntimeException) extends RuntimeException(ex) {
  def this() = this(new RuntimeException("Error not specified"))
  def this(throwable: Throwable) = this(new RuntimeException("Error not specified", throwable))
}