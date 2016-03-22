package ch.unibas.dmi.dbis.adam.evaluation.config
import com.typesafe.config.ConfigFactory

/**
  * adamtwo
  *
  * Ivan Giangreco
  * August 2015
  */
object EvaluationConfig{
  val config = ConfigFactory.load()
  config.checkValid(ConfigFactory.defaultReference(), "adampro")

  val evaluationPath = config.getString("adampro.evaluationPath")

  val collectionSizes = Seq(10000, 50000, 100000, 500000, 1000000, 5000000, 10000000, 20000000, 50000000)
  val vectorSizes = Seq(10, 50, 100, 200, 500, 1000)

  val indexes = IndexTypes.values

  val k = 100

  val norm = 1

  val numExperiments = 10
}
