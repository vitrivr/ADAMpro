package org.vitrivr.adampro.query.planner

import breeze.linalg.DenseVector
import org.vitrivr.adampro.communication.api.QueryOp
import org.vitrivr.adampro.data.datatypes.TupleID._
import org.vitrivr.adampro.data.datatypes.vector.Vector
import org.vitrivr.adampro.data.datatypes.vector.Vector._
import org.vitrivr.adampro.data.entity.Entity
import org.vitrivr.adampro.query.tracker.QueryTracker
import org.vitrivr.adampro.data.index.structures.ecp.ECPIndex
import org.vitrivr.adampro.data.index.structures.lsh.LSHIndex
import org.vitrivr.adampro.data.index.structures.mi.MIIndex
import org.vitrivr.adampro.data.index.structures.pq.PQIndex
import org.vitrivr.adampro.data.index.structures.sh.SHIndex
import org.vitrivr.adampro.data.index.structures.va.{VAIndex, VAPlusIndex, VAPlusIndexMetaData}
import org.vitrivr.adampro.data.index.{Index, IndexingTaskTuple}
import org.vitrivr.adampro.process.SharedComponentContext
import org.vitrivr.adampro.utils.ml.{Regression, PegasosSVM, TrainingSample}
import org.vitrivr.adampro.query.query.RankingQuery

import scala.collection.mutable.ListBuffer

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * November 2016
  */
private[planner] class SVMPlannerHeuristics(defaultNRuns: Int = 100) extends PlannerHeuristics("svm", defaultNRuns) {
  /**
    *
    * @param indexes
    * @param queries
    * @param options
    */
  override def trainIndexes(indexes: Seq[Index], queries: Seq[RankingQuery], options: Map[String, String] = Map())(implicit ac : SharedComponentContext): Unit = {
    val entity = indexes.head.entity.get
    val tracker = new QueryTracker()

    val trainData = queries.flatMap { nnq =>
      val rel = QueryOp.sequential(entity.entityname, nnq, None)(tracker).get.get.select(entity.pk.name).collect().map(_.getAs[Any](0)).toSet

      indexes.map { index =>
        performMeasurement(index, nnq, options.get("nruns").map(_.toInt), rel).map(measurement => (index.indextypename, buildFeature(index, nnq, measurement.toConfidence()), measurement))
      }.flatten
    }.groupBy(_._1).mapValues(_.map(x => (x._2, x._3)))

    trainData.foreach { case (indextypename, trainDatum) =>

      if (trainDatum.nonEmpty && !ac.catalogManager.containsOptimizerOptionMeta(name, "svm-index-" + indextypename.name).getOrElse(false)) {
        ac.catalogManager.createOptimizerOption(name, "svm-index-" + indextypename.name, new PegasosSVM(trainDatum.head._1.length))
      }

      val svm = ac.catalogManager.getOptimizerOptionMeta(name, "svm-index-" + indextypename.name).get.asInstanceOf[PegasosSVM]
      svm.train(trainDatum.map { case (x, y) => TrainingSample(x, y.time) })
      ac.catalogManager.updateOptimizerOption(name, "svm-index-" + indextypename.name, svm)
    }

    tracker.cleanAll()
  }

  /**
    *
    * @param entity
    * @param queries
    * @param options
    */
  override def trainEntity(entity: Entity, queries: Seq[RankingQuery], options: Map[String, String] = Map())(implicit ac : SharedComponentContext): Unit = {
    val trainDatum = queries.flatMap { nnq =>
      performMeasurement(entity, nnq, options.get("nruns").map(_.toInt)).map(measurement => (buildFeature(entity, nnq, measurement.toConfidence()), measurement))
    }

    if (trainDatum.nonEmpty && !ac.catalogManager.containsOptimizerOptionMeta(name, "svm-entity").getOrElse(false)) {
      ac.catalogManager.createOptimizerOption(name, "svm-entity", new PegasosSVM(trainDatum.head._1.length))
    }

    val svm = ac.catalogManager.getOptimizerOptionMeta(name, "svm-entity").get.asInstanceOf[PegasosSVM]
    svm.train(trainDatum.map { case (x, y) => TrainingSample(x, y.time) })

    ac.catalogManager.updateOptimizerOption(name, "svm-entity", svm)
  }

  /**
    *
    * @param index
    * @param nnq
    */
  override def test(index: Index, nnq: RankingQuery)(implicit ac : SharedComponentContext): Double = {
    val confidence = nnq.options.get("confidence").map(_.toDouble).getOrElse(1.0)

    getScore("svm-index-" + index.indextypename.name, buildFeature(index, nnq, Confidence(confidence)))(ac)
  }


  /**
    *
    * @param entity
    * @param nnq
    * @return
    */
  override def test(entity: Entity, nnq: RankingQuery)(implicit ac : SharedComponentContext): Double = {
    val confidence = nnq.options.get("confidence").map(_.toDouble).getOrElse(1.0)

    getScore("svm-entity", buildFeature(entity, nnq, Confidence(confidence)))(ac)
  }


  /**
    *
    * @param key
    * @param f
    * @return
    */
  private def getScore(key: String, f: DenseVector[Double])(implicit ac : SharedComponentContext): Double = {
    if (ac.catalogManager.containsOptimizerOptionMeta(name, key).getOrElse(false)) {
      val metaOpt = ac.catalogManager.getOptimizerOptionMeta(name, key)

      if (metaOpt.isSuccess) {
        val svm = metaOpt.get.asInstanceOf[PegasosSVM]
        val ypred = svm.test(f)

        log.info("SVM optimizer predicted " + ypred)

        1 / ypred // 1 / ypred as score is better the higher, but ypred gets worse the higher the value
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
    * @param nnq
    * @param confidence
    */
  private def buildFeature(index: Index, nnq: RankingQuery, confidence : Confidence): DenseVector[Double] = DenseVector[Double]((buildFeature(confidence) ++ buildFeature(index, nnq)).toArray)

  /**
    *
    * @param entity
    * @param nnq
    * @param confidence
    */
  private def buildFeature(entity: Entity, nnq: RankingQuery, confidence : Confidence)(implicit ac : SharedComponentContext): DenseVector[Double] = DenseVector[Double]((buildFeature(confidence) ++ buildFeature(entity, nnq)).toArray)


  /**
    *
    * @param index
    * @param nnq
    */
  private def buildFeature(index: Index, nnq: RankingQuery): Seq[Double] = buildFeature(index) ++ buildFeature(nnq)

  /**
    *
    * @param entity
    * @param nnq
    */
  private def buildFeature(entity: Entity, nnq: RankingQuery)(implicit ac : SharedComponentContext): Seq[Double] = buildFeature(entity, nnq.attribute) ++ buildFeature(nnq)

  /**
    *
    * @param confidence
    * @return
    */
  private def buildFeature(confidence: Confidence): Seq[Double] = {
    val lb = new ListBuffer[Double]()

    //lb += math.min(1.0, confidence.confidence)

    lb.toSeq
  }

  /**
    *
    * @param nnq
    * @return
    */
  private def buildFeature(nnq: RankingQuery): Seq[Double] = {
    val lb = new ListBuffer[Double]()

    lb += math.min(1.0, nnq.k / 1000.0)

    lb.toSeq
  }

  /**
    *
    * @param entity
    * @param attribute
    * @return
    */
  private def buildFeature(entity: Entity, attribute: String)(implicit ac : SharedComponentContext): Seq[Double] = {
    val lb = new ListBuffer[Double]()
    val head = entity.getAttributeData(attribute).get.head()

    lb += math.min(1.0, IndexingTaskTuple(head.getAs[TupleID](entity.pk.name), Vector.conv_draw2vec(head.getAs[DenseRawVector](attribute))).ap_indexable.length / 10000.0)
    lb += math.min(1.0, entity.count / 500000000.0)

    lb.toSeq
  }

  /**
    *
    * @param index
    * @return
    */
  private def buildFeature(index: Index): Seq[Double] = {
    val lb = new ListBuffer[Double]()

    lb += math.min(1.0, index.count / 500000000.0)

    lb ++= (index match {
      case idx: ECPIndex => buildFeature(idx)
      case idx: LSHIndex => buildFeature(idx)
      case idx: MIIndex => buildFeature(idx)
      case idx: PQIndex => buildFeature(idx)
      case idx: SHIndex => buildFeature(idx)
      case idx: VAPlusIndex => buildFeature(idx)
      case idx: VAIndex => buildFeature(idx)
      case _ => Seq()
    })

    lb.toSeq
  }

  /**
    *
    * @param index
    * @return
    */
  private def buildFeature(index: ECPIndex): Seq[Double] = {
    val lb = new ListBuffer[Double]()
    val meta = index.meta

    lb += math.min(1.0, meta.leaders.length / 100000.0)

    lb.toSeq
  }

  /**
    *
    * @param index
    * @return
    */
  private def buildFeature(index: LSHIndex): Seq[Double] = {
    val lb = new ListBuffer[Double]()
    val meta = index.meta

    lb += math.min(1.0, meta.ghashf.length / 1000.0)
    lb += math.min(1.0, meta.m / 10000.0)
    lb += math.min(1.0, meta.radius / 1000.0)
    lb += math.min(1.0, meta.ghashf.head.hhashf.size / 10000.0)

    lb.toSeq
  }

  /**
    *
    * @param index
    * @return
    */
  private def buildFeature(index: MIIndex): Seq[Double] = {
    val lb = new ListBuffer[Double]()
    val meta = index.meta

    lb += math.min(1.0, meta.ki / 10000.0)
    lb += math.min(1.0, meta.ks / 10000.0)
    lb += math.min(1.0, meta.refs.length / 1000.0)

    lb.toSeq
  }

  /**
    *
    * @param index
    * @return
    */
  private def buildFeature(index: PQIndex): Seq[Double] = {
    val lb = new ListBuffer[Double]()
    val meta = index.meta

    lb += math.min(1.0, meta.models.length / 10000.0)
    lb += math.min(1.0, meta.nsq / 500.0)

    lb.toSeq
  }

  /**
    *
    * @param index
    * @return
    */
  private def buildFeature(index: SHIndex): Seq[Double] = {
    val lb = new ListBuffer[Double]()
    val meta = index.meta

    lb += math.min(1.0, meta.eigenfunctions.size / 10000.0)

    lb.toSeq
  }

  /**
    *
    * @param index
    * @return
    */
  private def buildFeature(index: VAIndex): Seq[Double] = {
    val lb = new ListBuffer[Double]()
    val meta = index.meta

    lb += math.min(1.0, meta.marks.length / 50000.0)

    lb.toSeq
  }

  /**
    *
    * @param index
    * @return
    */
  private def buildFeature(index: VAPlusIndex): Seq[Double] = {
    val lb = new ListBuffer[Double]()
    val meta = index.meta.asInstanceOf[VAPlusIndexMetaData]

    lb += math.min(1.0, meta.marks.length / 50000.0)
    lb += math.min(1.0, meta.pca.getK / 10000.0)

    lb.toSeq
  }
}