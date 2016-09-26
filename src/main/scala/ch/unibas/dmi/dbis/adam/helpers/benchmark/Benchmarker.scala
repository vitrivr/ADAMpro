package ch.unibas.dmi.dbis.adam.helpers.benchmark

import ch.unibas.dmi.dbis.adam.api.QueryOp
import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.helpers.benchmark.ScanWeightCatalogOperator.{ScanWeightable, ScanWeightedEntity, ScanWeightedIndex}
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery
import ch.unibas.dmi.dbis.adam.utils.Logging

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * September 2016
  */
private[benchmark] class Benchmarker(indexes: Seq[Index], queries: Seq[NearestNeighbourQuery])(@transient implicit val ac: AdamContext) extends Serializable with Logging {
  //only one entity
  assert(indexes.map(_.entityname).distinct.length == 1)

  private val NRUNS = 100

  case class Measurement(precision: Float, recall: Float, time: Long)

  if(queries.length < 10){
    log.warn("only " + queries.length + " used for benchmarking; benchmarking results may not be significant")
  }


  /**
    *
    * @return
    */
  def getScores() = performMeasurement()


  /**
    *
    * @return
    */
  private def performMeasurement(): Map[ScanWeightable, Float] = {
    queries.flatMap {
      query => performMeasurement(query)
    }.groupBy(_._1).mapValues { x => x.map(_._2) }.mapValues { queryScores =>
      queryScores.sum / queryScores.length.toFloat
    }
  }


  /**
    *
    * @param nnq
    * @return
    */
  private def performMeasurement(nnq: NearestNeighbourQuery): Map[ScanWeightable, Float] = {
    val entity = indexes.head.entity.get
    val rel = QueryOp.sequential(entity.entityname, nnq, None).get.get.select(entity.pk.name).collect().map(_.getAs[Any](0)).toSet

    val indexScores = indexes.map { index => ScanWeightedIndex(index) -> totalScore(performMeasurement(index, nnq, rel)) }
    val entityScore = Seq(ScanWeightedEntity(entity, nnq.attribute) -> totalScore(performMeasurement(entity, nnq)))

    (indexScores ++ entityScore).toMap
  }

  /**
    *
    * @param entity
    * @param nnq
    * @return
    */
  private def performMeasurement(entity: Entity, nnq: NearestNeighbourQuery): Seq[Measurement] = {
    val entityNNQ = NearestNeighbourQuery(nnq.attribute, nnq.q, nnq.weights, nnq.distance, nnq.k, false, nnq.options, None)

    (0 until NRUNS).map { i =>
      val t1 = System.currentTimeMillis
      val res = QueryOp.sequential(entity.entityname, entityNNQ, None).get.get.select(entity.pk.name).collect()
      val t2 = System.currentTimeMillis

      val recall = 1.toFloat
      val precision = 1.toFloat
      val time = t2 - t1

      Measurement(precision, recall, time)
    }
  }


    /**
    *
    * @param index
    * @param nnq
    * @param rel
    * @return
    */
  private def performMeasurement(index: Index, nnq: NearestNeighbourQuery, rel: Set[Any]): Seq[Measurement] = {
    val indexOnlyNNQ = NearestNeighbourQuery(nnq.attribute, nnq.q, nnq.weights, nnq.distance, nnq.k, true, nnq.options, None)
    val entityN = index.entity.get.count


    (0 until NRUNS).map { i =>
      val t1 = System.currentTimeMillis
      val res = QueryOp.index(index.indexname, indexOnlyNNQ, None).get.get.select(index.entity.get.pk.name).collect()
      val t2 = System.currentTimeMillis

      val ret = res.map(_.getAs[Any](0)).toSet

      val recall = rel.intersect(ret).size / rel.size
      val precision = rel.intersect(ret).size / ret.size
      val time = t2 - t1


      Measurement(precision, recall, time)
    }
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