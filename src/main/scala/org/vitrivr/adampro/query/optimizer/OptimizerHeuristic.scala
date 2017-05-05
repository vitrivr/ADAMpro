package org.vitrivr.adampro.query.optimizer

import org.vitrivr.adampro.api.QueryOp
import org.vitrivr.adampro.entity.Entity
import org.vitrivr.adampro.helpers.tracker.OperationTracker
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
private[optimizer] abstract class OptimizerHeuristic(protected val name : String, private val defaultNRuns : Int = 100)(@transient implicit val ac: AdamContext) extends Serializable with Logging {

  case class Measurement(tp : Int, precision: Double, recall: Double, time: Double){
    def toConfidence() : Confidence = Confidence(2 * (precision * recall) / (precision + recall))
  }
  case class Confidence(confidence : Double)

  /**
    *
    * @param entity
    * @param queries
    */
  def trainEntity(entity: Entity, queries: Seq[NearestNeighbourQuery], options : Map[String, String] = Map()): Unit


  /**
    *
    * @param indexes
    * @param queries
    */
  def trainIndexes(indexes: Seq[Index], queries: Seq[NearestNeighbourQuery], options : Map[String, String] = Map()): Unit

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
  protected def performMeasurement(entity: Entity, nnq: NearestNeighbourQuery, nruns : Option[Int]): Seq[Measurement] = {
    val entityNNQ = NearestNeighbourQuery(nnq.attribute, nnq.q, nnq.weights, nnq.distance, nnq.k, false, nnq.options, None)
    val tracker = new OperationTracker()

    val res = (0 until nruns.getOrElse(defaultNRuns)).map {
      i =>
        val t1 = System.currentTimeMillis
        val res = QueryOp.sequential(entity.entityname, entityNNQ, None)(tracker).get.get.select(entity.pk.name).collect()
        val t2 = System.currentTimeMillis

        val recall = 1.toFloat
        val precision = 1.toFloat
        val time = t2 - t1

        Measurement(nnq.k, precision, recall, time)
    }

    tracker.cleanAll()

    res
  }


  /**
    *
    * @param index
    * @param nnq
    * @param rel
    * @return
    */
  protected def performMeasurement(index: Index, nnq: NearestNeighbourQuery, nruns : Option[Int], rel: Set[Any]): Seq[Measurement] = {
    val indexOnlyNNQ = NearestNeighbourQuery(nnq.attribute, nnq.q, nnq.weights, nnq.distance, nnq.k, true, nnq.options, None)
    val entityN = index.entity.get.count
    val tracker = new OperationTracker()

    val res = (0 until nruns.getOrElse(defaultNRuns)).map {
      i =>
        val t1 = System.currentTimeMillis
        val res = QueryOp.index(index.indexname, indexOnlyNNQ, None)(tracker).get.get.select(index.entity.get.pk.name).collect()
        val t2 = System.currentTimeMillis

        val ret = res.map(_.getAs[Any](0)).toSet

        val tp = rel.intersect(ret).size

        val nrelevant = rel.size
        val nretrieved = ret.size

        val recall = tp.toDouble / nrelevant.toDouble
        val precision = tp.toDouble / nretrieved.toDouble
        val time = t2 - t1


        Measurement(tp, precision, recall, time)
    }

    tracker.cleanAll()

    res
  }
}
