package org.vitrivr.adampro.helpers.benchmark

import org.vitrivr.adampro.entity.Entity
import org.vitrivr.adampro.index.Index
import org.vitrivr.adampro.main.AdamContext
import org.vitrivr.adampro.query.query.NearestNeighbourQuery

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * November 2016
  */
private[benchmark] class NaiveBenchmarker(indexes: Seq[Index], queries: Seq[NearestNeighbourQuery])(@transient implicit override val ac: AdamContext) extends Benchmarker(indexes, queries)(ac) {

  /**
    * @param index
    * @param measurements
    * @return
    */
  override protected def totalScore(index : Index, measurements: Seq[Measurement]): Float = {
    totalScore(measurements)
  }

  /**
    * @param entity
    * @param measurements
    * @return
    */
  override protected def totalScore(entity : Entity, measurements: Seq[Measurement]): Float = {
    totalScore(measurements)
  }

  /**
    * Computes a score per scan method. The higher the score the better the scan method.
    *
    * @param measurements
    * @return
    */
  private def totalScore(measurements: Seq[Measurement]): Float = {
    val _timeScore = timeScore(measurements.map(_.time))
    val _precisionScore = precisionScore(measurements.map(_.precision))
    val _recallScore = recallScore(measurements.map(_.recall))

    val scores = Seq(_timeScore, _precisionScore, _recallScore)


    scores.sum.toFloat / scores.length.toFloat
  }

  /**
    *
    * @param measurements
    * @return
    */
  private def timeScore(measurements: Seq[Long]): Float = {
    val mean = measurements.sum / measurements.length.toFloat
    val stdev = math.sqrt(measurements.map { measure => (measure - mean) * (measure - mean) }.sum / measurements.length.toFloat).toFloat

    //remove measurements > or < 3 * stdev
    val filteredMeasurements = measurements.filterNot(m => m > mean + 3 * stdev).filterNot(m => m < mean - 3 * stdev)

    //scoring function
    val maxTime = 10
    val score = (x : Float) => 1 / (1 + maxTime * math.exp(-0.5 * x)) //TODO: normalize by other measurements too?

    val scores = filteredMeasurements.map(x => score(x.toFloat))

    //average
    scores.sum.toFloat / scores.length.toFloat
  }


  /**
    *
    * @param measurements
    * @return
    */
  private def precisionScore(measurements: Seq[Float]): Float = {
    val mean = measurements.sum / measurements.length.toFloat
    val stdev = math.sqrt(measurements.map { measure => (measure - mean) * (measure - mean) }.sum / measurements.length.toFloat).toFloat

    //remove measurements > or < 3 * stdev
    val filteredMeasurements = measurements.filterNot(m => m > mean + 3 * stdev).filterNot(m => m < mean - 3 * stdev)

    //average
    filteredMeasurements.sum / filteredMeasurements.length.toFloat
  }

  /**
    *
    * @param measurements
    * @return
    */
  private def recallScore(measurements: Seq[Float]): Float = {
    val mean = measurements.sum / measurements.length.toFloat
    val stdev = math.sqrt(measurements.map { measure => (measure - mean) * (measure - mean) }.sum / measurements.length.toFloat).toFloat

    //remove measurements > or < 3 * stdev
    val filteredMeasurements = measurements.filterNot(m => m > mean + 3 * stdev).filterNot(m => m < mean - 3 * stdev)

    //average
    filteredMeasurements.sum / filteredMeasurements.length.toFloat
  }

}
