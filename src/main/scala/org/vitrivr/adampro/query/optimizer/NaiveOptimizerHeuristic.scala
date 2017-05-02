package org.vitrivr.adampro.query.optimizer

import org.vitrivr.adampro.api.QueryOp
import org.vitrivr.adampro.entity.Entity
import org.vitrivr.adampro.helpers.tracker.OperationTracker
import org.vitrivr.adampro.index.Index
import org.vitrivr.adampro.main.{AdamContext, SparkStartup}
import org.vitrivr.adampro.query.query.NearestNeighbourQuery

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * September 2016
  */
private[optimizer] class NaiveOptimizerHeuristic(defaultNRuns: Int = 100)(@transient implicit override val ac: AdamContext) extends OptimizerHeuristic("naive", defaultNRuns) {
  /**
    *
    * @param indexes
    * @param queries
    * @param options
    */
  override def trainIndexes(indexes: Seq[Index], queries: Seq[NearestNeighbourQuery], options: Map[String, String] = Map()): Unit = {
    val entity = indexes.head.entity.get

    val tracker = new OperationTracker()

    queries.flatMap { nnq =>
      val rel = QueryOp.sequential(entity.entityname, nnq, None)(tracker).get.get.select(entity.pk.name).collect().map(_.getAs[Any](0)).toSet

      indexes.map { index =>
        index -> performMeasurement(index, nnq, options.get("nruns").map(_.toInt), rel)
      }
    }.groupBy(_._1).mapValues {
      _.map(_._2)
    }.foreach { case (index, measurements) =>
      val scores = totalScore(index, measurements.flatten).toArray
      SparkStartup.catalogOperator.createOptimizerOption(name, "scores-index-" + index.indexname, scores)
    }

    tracker.cleanAll()
  }

  /**
    *
    * @param entity
    * @param queries
    * @param options
    */
  override def trainEntity(entity: Entity, queries: Seq[NearestNeighbourQuery], options: Map[String, String] = Map()): Unit = {
    val measurements = queries.flatMap { nnq =>
      performMeasurement(entity, nnq, options.get("nruns").map(_.toInt))
    }

    val scores = totalScore(entity, measurements).toArray
    SparkStartup.catalogOperator.createOptimizerOption(name, "scores-entity-" + entity.entityname, scores)
  }


  /**
    *
    * @param index
    * @param nnq
    * @return
    */
  override def test(index: Index, nnq: NearestNeighbourQuery): Double = getScore("scores-index-" + index.indexname)

  /**
    *
    * @param entity
    * @param nnq
    * @return
    */
  override def test(entity: Entity, nnq: NearestNeighbourQuery): Double = getScore("scores-entity-" + entity.entityname)


  /**
    *
    * @param key
    * @return
    */
  private def getScore(key: String): Double = {
    if (SparkStartup.catalogOperator.containsOptimizerOptionMeta(name, key).getOrElse(false)) {
      val metaOpt = SparkStartup.catalogOperator.getOptimizerOptionMeta(name, key)

      if (metaOpt.isSuccess) {
        val scores = metaOpt.get.asInstanceOf[Array[Double]]
        scores.sum / scores.length.toFloat
      } else {
        0.toDouble
      }
    } else {
      0.toDouble
    }
  }


  /**
    *
    * @param index
    * @param measurements
    * @return
    */
  private def totalScore(index: Index, measurements: Seq[Measurement]): Seq[Double] = totalScore(measurements)

  /**
    *
    * @param entity
    * @param measurements
    * @return
    */
  private def totalScore(entity: Entity, measurements: Seq[Measurement]): Seq[Double] = totalScore(measurements)


  /**
    *
    * @param measurements
    * @return
    */
  private def totalScore(measurements: Seq[Measurement]): Seq[Double] = {
    val _timeScore = timeScore(measurements.map(_.time))
    val _precisionScore = precisionScore(measurements.map(_.precision))
    val _recallScore = recallScore(measurements.map(_.recall))

    Seq(_timeScore, _precisionScore, _recallScore)
  }

  /**
    *
    * @param vals
    * @return
    */
  private def normalizedAverage(vals: Seq[Double]): Double = {
    val mean = vals.sum / vals.length.toFloat
    val stdev = math.sqrt(vals.map {
      measure => (measure - mean) * (measure - mean)
    }.sum / vals.length.toFloat).toFloat

    //remove measurements > or < 3 * stdev
    val filteredMeasurements = vals.filterNot(m => m > mean + 3 * stdev).filterNot(m => m < mean - 3 * stdev)

    //scoring function
    val maxTime = 10
    val score = (x: Float) => 1 / (1 + maxTime * math.exp(-0.5 * x)) //TODO: normalize by other measurements too?

    val scores = filteredMeasurements.map(x => score(x.toFloat))

    //average
    scores.sum / scores.length.toDouble
  }

  /**
    *
    * @param measurements
    * @return
    */
  private def timeScore(measurements: Seq[Double]): Double = normalizedAverage(measurements.map(_.toDouble))

  /**
    *
    * @param measurements
    * @return
    */
  private def precisionScore(measurements: Seq[Double]): Double = normalizedAverage(measurements)

  /**
    *
    * @param measurements
    * @return
    */
  private def recallScore(measurements: Seq[Double]): Double = normalizedAverage(measurements)
}