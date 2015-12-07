package ch.unibas.dmi.dbis.adam.evaluation

import ch.unibas.dmi.dbis.adam.index.structures.IndexStructures

/**
 * adamtwo
 *
 * Ivan Giangreco
 * November 2015
 */
object EvaluationConfig {
  val dbSizes = Seq(1000, 10000, 50000, 100000, 500000, 1000000, 5000000, 10000000, 20000000, 50000000)
  val vectorSizes = Seq(10, 50, 100, 200, 500)

  val indexes = Seq(IndexStructures.LSH, IndexStructures.SH, IndexStructures.VAF, IndexStructures.VAV, IndexStructures.ECP)

  val k = 100
  val numExperiments = 10
}
