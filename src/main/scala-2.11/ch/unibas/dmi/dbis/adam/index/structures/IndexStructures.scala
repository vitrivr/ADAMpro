package ch.unibas.dmi.dbis.adam.index.structures

/**
 * adamtwo
 *
 * Ivan Giangreco
 * October 2015
 */
object IndexStructures extends Enumeration {
  val ECP = Value("ecp")
  val LSH = Value("lsh")
  val SH = Value("sh")
  val VAF = Value("vaf")
  val VAV = Value("vav")
}