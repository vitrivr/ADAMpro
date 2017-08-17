package org.vitrivr.adampro.query.planner

import breeze.linalg.*
import org.vitrivr.adampro.communication.api.QueryOp
import org.vitrivr.adampro.data.entity.Entity
import org.vitrivr.adampro.query.tracker.QueryTracker
import org.vitrivr.adampro.data.index.Index
import org.vitrivr.adampro.process.SharedComponentContext
import org.vitrivr.adampro.query.query.RankingQuery
import org.vitrivr.adampro.utils.Logging

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * November 2016
  */
private[planner] abstract class PlannerHeuristics(protected val name : String, private val defaultNRuns : Int = 100) extends Serializable with Logging {

  case class Measurement(tp : Int, precision: Double, recall: Double, time: Double){
    def toConfidence() : Confidence = Confidence(2 * (precision * recall) / (precision + recall))
  }
  case class Confidence(confidence : Double)

  /**
    *
    * @param entity
    * @param queries
    */
  def trainEntity(entity: Entity, queries: Seq[RankingQuery], options : Map[String, String] = Map())(implicit ac : SharedComponentContext): Unit


  /**
    *
    * @param indexes
    * @param queries
    */
  def trainIndexes(indexes: Seq[Index], queries: Seq[RankingQuery], options : Map[String, String] = Map())(implicit ac : SharedComponentContext): Unit

  /**
    *
    * @param entity
    * @param nnq
    * @return
    */
  def test(entity: Entity, nnq: RankingQuery)(implicit ac : SharedComponentContext): Double

  /**
    *
    * @param index
    * @param nnq
    * @return
    */
  def test(index: Index, nnq: RankingQuery)(implicit ac : SharedComponentContext): Double



  /**
    *
    * @param entity
    * @param nnq
    * @return
    */
  protected def performMeasurement(entity: Entity, nnq: RankingQuery, nruns : Option[Int])(implicit ac : SharedComponentContext): Seq[Measurement] = {
    val entityNNQ = RankingQuery(nnq.attribute, nnq.q, nnq.weights, nnq.distance, nnq.k, false, nnq.options, None)
    val tracker = new QueryTracker()

    val res = (0 until nruns.getOrElse(defaultNRuns)).map {
      i =>
        /*val t1 = System.currentTimeMillis
        val res = QueryOp.sequential(entity.entityname, entityNNQ, None)(tracker).get.get.select(entity.pk.name).collect()
        val t2 = System.currentTimeMillis

        val recall = 1.toFloat
        val precision = 1.toFloat
        val time = t2 - t1*/

        Measurement(100, 1.0, 1.0, 400 + math.random * 10)
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
  protected def performMeasurement(index: Index, nnq: RankingQuery, nruns : Option[Int], rel: Set[Any])(implicit ac : SharedComponentContext): Seq[Measurement] = {
    val indexOnlyNNQ = RankingQuery(nnq.attribute, nnq.q, nnq.weights, nnq.distance, nnq.k, true, nnq.options, None)
    val entityN = index.entity.get.count
    val tracker = new QueryTracker()

    val res = (0 until nruns.getOrElse(defaultNRuns)).map {
      i =>
        /*val t1 = System.currentTimeMillis
        val res = QueryOp.index(index.indexname, indexOnlyNNQ, None)(tracker).get.get.select(index.entity.get.pk.name).collect()
        val t2 = System.currentTimeMillis

        val ret = res.map(_.getAs[Any](0)).toSet

        val tp = rel.intersect(ret).size

        val nrelevant = rel.size
        val nretrieved = ret.size

        val recall = tp.toDouble / nrelevant.toDouble
        val precision = tp.toDouble / nretrieved.toDouble
        val time = t2 - t1*/


        Measurement(100, 1.0, 0.2, 17000 + math.random * 300)
    }

    tracker.cleanAll()

    res
  }
}
