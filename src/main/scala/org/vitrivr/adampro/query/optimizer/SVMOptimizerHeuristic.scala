package org.vitrivr.adampro.query.optimizer

import breeze.linalg.DenseVector
import org.vitrivr.adampro.api.QueryOp
import org.vitrivr.adampro.datatypes.TupleID._
import org.vitrivr.adampro.datatypes.vector.Vector
import org.vitrivr.adampro.datatypes.vector.Vector._
import org.vitrivr.adampro.entity.Entity
import org.vitrivr.adampro.helpers.tracker.OperationTracker
import org.vitrivr.adampro.index.structures.ecp.ECPIndex
import org.vitrivr.adampro.index.structures.lsh.LSHIndex
import org.vitrivr.adampro.index.structures.mi.MIIndex
import org.vitrivr.adampro.index.structures.pq.PQIndex
import org.vitrivr.adampro.index.structures.sh.SHIndex
import org.vitrivr.adampro.index.structures.va.{VAIndex, VAPlusIndex, VAPlusIndexMetaData}
import org.vitrivr.adampro.index.{Index, IndexingTaskTuple}
import org.vitrivr.adampro.main.{AdamContext, SparkStartup}
import org.vitrivr.adampro.ml.{PegasosSVM, TrainingSample}
import org.vitrivr.adampro.query.query.NearestNeighbourQuery

import scala.collection.mutable.ListBuffer

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * November 2016
  */
private[optimizer] class SVMOptimizerHeuristic(defaultNRuns: Int = 100)(@transient implicit override val ac: AdamContext) extends OptimizerHeuristic("svm", defaultNRuns) {
  /**
    *
    * @param indexes
    * @param queries
    * @param options
    */
  override def trainIndexes(indexes: Seq[Index], queries: Seq[NearestNeighbourQuery], options: Map[String, String] = Map()): Unit = {
    val entity = indexes.head.entity.get
    val tracker = new OperationTracker()

    val trainData = queries.flatMap { nnq =>
      val rel = QueryOp.sequential(entity.entityname, nnq, None)(tracker).get.get.select(entity.pk.name).collect().map(_.getAs[Any](0)).toSet

      indexes.map { index =>
        performMeasurement(index, nnq, options.get("nruns").map(_.toInt), rel).map(measurement => (index.indextypename, buildFeature(index, nnq, measurement.toConfidence()), measurement))
      }.flatten
    }.groupBy(_._1).mapValues(_.map(x => (x._2, x._3)))

    trainData.foreach { case (indextypename, trainDatum) =>
      if (trainDatum.nonEmpty && !SparkStartup.catalogOperator.containsOptimizerOptionMeta(name, "svm-index-" + indextypename.name).getOrElse(false)) {
        SparkStartup.catalogOperator.createOptimizerOption(name, "svm-index-" + indextypename.name, new PegasosSVM(trainDatum.head._1.length))
      }

      val svm = SparkStartup.catalogOperator.getOptimizerOptionMeta(name, "svm-index-" + indextypename.name).get.asInstanceOf[PegasosSVM]
      svm.train(trainDatum.map { case (x, y) => TrainingSample(x, y.time) })
      SparkStartup.catalogOperator.updateOptimizerOption(name, "svm-index-" + indextypename.name, svm)
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
    val trainDatum = queries.flatMap { nnq =>
      performMeasurement(entity, nnq, options.get("nruns").map(_.toInt)).map(measurement => (buildFeature(entity, nnq, measurement.toConfidence()), measurement))
    }

    if (trainDatum.nonEmpty && !SparkStartup.catalogOperator.containsOptimizerOptionMeta(name, "svm-entity").getOrElse(false)) {
      SparkStartup.catalogOperator.createOptimizerOption(name, "svm-entity", new PegasosSVM(trainDatum.head._1.length))
    }

    val svm = SparkStartup.catalogOperator.getOptimizerOptionMeta(name, "svm-entity").get.asInstanceOf[PegasosSVM]
    svm.train(trainDatum.map { case (x, y) => TrainingSample(x, y.time) })
    SparkStartup.catalogOperator.updateOptimizerOption(name, "svm-entity", svm)
  }

  /**
    *
    * @param index
    * @param nnq
    */
  override def test(index: Index, nnq: NearestNeighbourQuery): Double = {
    val confidence = nnq.options.get("confidence").map(_.toDouble).getOrElse(1.0)

    getScore("svm-index-" + index.indextypename.name, buildFeature(index, nnq, Confidence(confidence)))
  }


  /**
    *
    * @param entity
    * @param nnq
    * @return
    */
  override def test(entity: Entity, nnq: NearestNeighbourQuery): Double = {
    val confidence = nnq.options.get("confidence").map(_.toDouble).getOrElse(1.0)

    getScore("svm-entity", buildFeature(entity, nnq, Confidence(confidence)))
  }


  /**
    *
    * @param key
    * @param f
    * @return
    */
  private def getScore(key: String, f: DenseVector[Double]): Double = {
    if (SparkStartup.catalogOperator.containsOptimizerOptionMeta(name, key).getOrElse(false)) {
      val metaOpt = SparkStartup.catalogOperator.getOptimizerOptionMeta(name, key)

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
  private def buildFeature(index: Index, nnq: NearestNeighbourQuery, confidence : Confidence): DenseVector[Double] = DenseVector[Double]((buildFeature(confidence) ++ buildFeature(index, nnq)).toArray)

  /**
    *
    * @param entity
    * @param nnq
    * @param confidence
    */
  private def buildFeature(entity: Entity, nnq: NearestNeighbourQuery, confidence : Confidence): DenseVector[Double] = DenseVector[Double]((buildFeature(confidence) ++ buildFeature(entity, nnq)).toArray)


  /**
    *
    * @param index
    * @param nnq
    */
  private def buildFeature(index: Index, nnq: NearestNeighbourQuery): Seq[Double] = buildFeature(index) ++ buildFeature(nnq)

  /**
    *
    * @param entity
    * @param nnq
    */
  private def buildFeature(entity: Entity, nnq: NearestNeighbourQuery): Seq[Double] = buildFeature(entity, nnq.attribute) ++ buildFeature(nnq)

  /**
    *
    * @param confidence
    * @return
    */
  private def buildFeature(confidence: Confidence): Seq[Double] = {
    val lb = new ListBuffer[Double]()

    lb += math.max(1.0, confidence.confidence)

    lb.toSeq
  }

  /**
    *
    * @param nnq
    * @return
    */
  private def buildFeature(nnq: NearestNeighbourQuery): Seq[Double] = {
    val lb = new ListBuffer[Double]()

    lb += math.max(1.0, nnq.k / 100.0)

    lb.toSeq
  }

  /**
    *
    * @param entity
    * @param attribute
    * @return
    */
  private def buildFeature(entity: Entity, attribute: String): Seq[Double] = {
    val lb = new ListBuffer[Double]()
    val head = entity.getAttributeData(attribute).get.head()

    lb += math.max(1.0, IndexingTaskTuple(head.getAs[TupleID](entity.pk.name), Vector.conv_draw2vec(head.getAs[DenseRawVector](attribute))).ap_indexable.length / 1000.0)
    lb += math.max(1.0, entity.count / 1000000.0)

    lb.toSeq
  }

  /**
    *
    * @param index
    * @return
    */
  private def buildFeature(index: Index): Seq[Double] = {
    val lb = new ListBuffer[Double]()

    lb += math.max(1.0, index.count / 1000000.0)

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

    lb += math.max(1.0, meta.leaders.length / 10000.0)

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

    lb += math.max(1.0, meta.ghashf.length / 100.0)
    lb += math.max(1.0, meta.m / 1000.0)
    lb += math.max(1.0, meta.radius / 10.0)
    lb += math.max(1.0, meta.ghashf.head.hhashf.size / 100.0)

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

    lb += math.max(1.0, meta.ki / 100.0)
    lb += math.max(1.0, meta.ks / 100.0)
    lb += math.max(1.0, meta.refs.length / 100.0)

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

    lb += math.max(1.0, meta.models.length / 100.0)
    lb += math.max(1.0, meta.nsq / 500.0)

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

    lb += math.max(1.0, meta.eigenfunctions.size / 100.0)

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

    lb += math.max(1.0, meta.marks.length / 500.0)

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

    lb += math.max(1.0, meta.marks.length / 500.0)
    lb += math.max(1.0, meta.pca.k / 100.0)

    lb.toSeq
  }
}