package org.vitrivr.adampro.helpers.benchmark

import org.vitrivr.adampro.api.QueryOp
import org.vitrivr.adampro.entity.Entity
import org.vitrivr.adampro.helpers.benchmark.ScanWeightCatalogOperator.{ScanWeightable, ScanWeightedEntity, ScanWeightedIndex}
import org.vitrivr.adampro.index.Index
import org.vitrivr.adampro.main.AdamContext
import org.vitrivr.adampro.query.query.NearestNeighbourQuery
import org.vitrivr.adampro.utils.Logging

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * September 2016
  */
private[benchmark] abstract class Benchmarker(indexes: Seq[Index], queries: Seq[NearestNeighbourQuery])(@transient implicit val ac: AdamContext) extends Serializable with Logging {
  //only one entity
  assert(indexes.map(_.entityname).distinct.length == 1)

  private val NRUNS = 100

  case class Measurement(nnq: NearestNeighbourQuery, precision: Float, recall: Float, time: Long)

  if (queries.length < 10) {
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

    val indexScores = indexes.map { index => ScanWeightedIndex(index) -> totalScore(index, performMeasurement(index, nnq, rel)) }
    val entityScore = Seq(ScanWeightedEntity(entity, nnq.attribute) -> totalScore(entity, performMeasurement(entity, nnq)))

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

      Measurement(nnq, precision, recall, time)
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


      Measurement(nnq, precision, recall, time)
    }
  }

  /**
    * Computes a score per scan method. The higher the score the better the scan method.
    *
    * @param index
    * @param measurements
    * @return
    */
  protected def totalScore(index: Index, measurements: Seq[Measurement]): Float


  /**
    * Computes a score per scan method. The higher the score the better the scan method.
    *
    * @param entity
    * @param measurements
    * @return
    */
  protected def totalScore(entity: Entity, measurements: Seq[Measurement]): Float

}