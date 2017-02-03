package org.vitrivr.adampro.query.optimizer

import org.vitrivr.adampro.api.QueryOp
import org.vitrivr.adampro.entity.Entity
import org.vitrivr.adampro.index.Index
import org.vitrivr.adampro.main.AdamContext
import org.vitrivr.adampro.query.query.NearestNeighbourQuery
import org.vitrivr.adampro.utils.Logging

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * November 2016
  */
private[optimizer] abstract class OptimizerHeuristic(protected val name : String, private val nruns : Int = 100)(@transient implicit val ac: AdamContext) extends Serializable with Logging {

  case class Measurement(precision: Double, recall: Double, time: Double)

  /**
    *
    * @param entity
    * @param queries
    */
  def train(entity: Entity, queries: Seq[NearestNeighbourQuery]): Unit


  /**
    *
    * @param indexes
    * @param queries
    */
  def train(indexes: Seq[Index], queries: Seq[NearestNeighbourQuery]): Unit

  /**
    *
    * @param entity
    * @param nnq
    * @return
    */
  def test(entity: Entity, nnq: NearestNeighbourQuery): Double

  /**
    *
    * @param index
    * @param nnq
    * @return
    */
  def test(index: Index, nnq: NearestNeighbourQuery): Double



  /**
    *
    * @param entity
    * @param nnq
    * @return
    */
  protected def performMeasurement(entity: Entity, nnq: NearestNeighbourQuery): Seq[Measurement] = {
    val entityNNQ = NearestNeighbourQuery(nnq.attribute, nnq.q, nnq.weights, nnq.distance, nnq.k, false, nnq.options, None)

    (0 until nruns).map {
      i =>
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
  protected def performMeasurement(index: Index, nnq: NearestNeighbourQuery, rel: Set[Any]): Seq[Measurement] = {
    val indexOnlyNNQ = NearestNeighbourQuery(nnq.attribute, nnq.q, nnq.weights, nnq.distance, nnq.k, true, nnq.options, None)
    val entityN = index.entity.get.count


    (0 until nruns).map {
      i =>
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
}
