package ch.unibas.dmi.dbis.adam.helpers.benchmark

import ch.unibas.dmi.dbis.adam.api.QueryOp
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.index.Index.IndexName
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery
import ch.unibas.dmi.dbis.adam.utils.Logging

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * September 2016
  */
class BenchmarkMeasurer(indexes: Seq[Index], queries: Seq[NearestNeighbourQuery])(@transient implicit val ac: AdamContext) extends Serializable with Logging {
  assert(indexes.map(_.entityname).distinct.length == 1)
  //only one entity

  private val NRUNS = 100

  case class Measurement(precision: Float, recall: Float, time: Long)

  //TODO: consider entity scan too


  /**
    *
    * @return
    */
  def getScores() = performMeasurement()


  /**
    *
    * @return
    */
  private def performMeasurement(): Map[IndexName, Float] = {
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
  private def performMeasurement(nnq: NearestNeighbourQuery): Map[IndexName, Float] = {
    val entity = indexes.head.entity.get
    val rel = QueryOp.sequential(entity.entityname, nnq, None).get.get.select(entity.pk.name).collect().map(_.getAs[Any](0)).toSet

    indexes.map { index =>
      index.indexname -> performMeasurement(index, nnq, rel)
    }.toMap.mapValues { measurements =>
      totalScore(measurements)
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

    //average
    filteredMeasurements.sum.toFloat / filteredMeasurements.length.toFloat
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
    filteredMeasurements.sum.toFloat / filteredMeasurements.length.toFloat
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
    filteredMeasurements.sum.toFloat / filteredMeasurements.length.toFloat
  }
}