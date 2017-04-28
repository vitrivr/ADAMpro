package org.vitrivr.adampro.chronos.utils

import scala.util.Random

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * April 2017
  */
object Helpers {
  /**
    * Generates a string (only a-z).
    *
    * @param nletters
    * @return
    */
  def generateString(nletters: Int) = (0 until nletters).map(x => Random.nextInt(26)).map(x => ('a' + x).toChar).mkString

}
