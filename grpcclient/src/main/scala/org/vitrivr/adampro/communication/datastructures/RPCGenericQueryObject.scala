package org.vitrivr.adampro.communication.datastructures

import org.vitrivr.adampro.grpc.grpc.QueryMessage.InformationLevel
import org.vitrivr.adampro.grpc.grpc.QueryMessage.InformationLevel.{INFORMATION_FULL_TREE, INFORMATION_INTERMEDIATE_RESULTS, INFORMATION_LAST_STEP_ONLY, WITH_PROVENANCE_PARTITION_INFORMATION, WITH_PROVENANCE_SOURCE_INFORMATION}
import org.vitrivr.adampro.grpc.grpc._

import scala.collection.mutable.ListBuffer

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * May 2017
  */
abstract class RPCGenericQueryObject(val id: String, val options: Map[String, String]) {
  /**
    *
    * @return
    */
  def buildQueryMessage: QueryMessage = {
    var qm = QueryMessage()
      .withQueryid(id)
      .withInformation(informationLevel)

    if (options.contains("projection")) {
      qm = qm.withProjection(projection)
    }

    if (options.contains("nofallback")) {
      qm.withNoFallback(options.get("nofallback").get.toBoolean)
    }

    setQueryMessage(qm)
  }


  /**
    *
    * @return
    */
  def prepare(): RPCGenericQueryObject = {
    this
  }

  /**
    *
    * @param qm
    * @return
    */
  protected def setQueryMessage(qm: QueryMessage): QueryMessage

  /**
    *
    * @return
    */
  def operation : String


  /**
    *
    * @param key
    * @return
    */
  private[datastructures] def getOption(key : String) =  options.get(key)


  /**
    *
    * @return
    */
  private[datastructures] def projection: ProjectionMessage = {
    throw new Exception("not implemented yet")
  }



  /**
    *
    * @return
    */
  private[datastructures] def informationLevel: Seq[InformationLevel] = {
    val option = options.getOrElse("informationlevel", "minimal")

    option match {
      case "all" => Seq(INFORMATION_FULL_TREE, INFORMATION_INTERMEDIATE_RESULTS, WITH_PROVENANCE_PARTITION_INFORMATION, WITH_PROVENANCE_SOURCE_INFORMATION)
      case "all_noprovenance" => Seq(INFORMATION_FULL_TREE, INFORMATION_INTERMEDIATE_RESULTS)
      case "minimal" => Seq(INFORMATION_LAST_STEP_ONLY)
      case _ => Seq(INFORMATION_LAST_STEP_ONLY)
    }
  }


  /**
    *
    * @return
    */
  private[datastructures] def nnq = {
    val partitions = if (options.get("partitions").isDefined) {
      val s = options.get("partitions").get
      if (s.contains(",")) {
        s.split(",").map(_.toInt).toSeq
      } else {
        Seq[Int](s.toInt)
      }
    } else {
      Seq[Int]()
    }

    val nnq = NearestNeighbourQueryMessage(options.getOrElse("attribute", "feature"), query,
      weights, Some(distance),
      options.get("k").getOrElse("100").toInt,
      options, //not overly clean solution, but not problematic to send too much information in this case
      options.get("indexonly").map(_.toBoolean).getOrElse(false),
      partitions)

    if (nnq.query.isEmpty) {
      None
    } else {
      Some(nnq)
    }
  }

  /**
    *
    * @return
    */
  private[datastructures] def query = {
    if (options.get("query").isDefined) {
      val vals = options.get("query").get.split(",").map(_.toFloat)

      if (options.get("sparsequery").map(_.toBoolean).getOrElse(false)) {
        val (vv, ii, size) = sparsify(vals)
        Some(VectorMessage().withSparseVector(SparseVectorMessage(ii, vv, size)))
      } else {
        Some(VectorMessage().withDenseVector(DenseVectorMessage(vals)))
      }
    } else {
      None
    }
  }

  /**
    *
    * @return
    */
  private[datastructures] def weights = {
    if (options.get("weights").isDefined && options.get("weights").get.length > 0) {
      val vals = options.get("weights").get.split(",").map(_.toFloat)

      if (options.get("sparseweights").map(_.toBoolean).getOrElse(false)) {
        val (vv, ii, size) = sparsify(vals)
        Some(VectorMessage().withSparseVector(SparseVectorMessage(ii, vv, size)))
      } else {
        Some(VectorMessage().withDenseVector(DenseVectorMessage(vals)))
      }
    } else {
      None
    }
  }


  /**
    *
    * @return
    */
  private[datastructures] def distance: DistanceMessage = {
    val distance = options.getOrElse("distance", "")

    distance match {
      case "chisquared" => DistanceMessage(DistanceMessage.DistanceType.chisquared)
      case "correlation" => DistanceMessage(DistanceMessage.DistanceType.correlation)
      case "cosine" => DistanceMessage(DistanceMessage.DistanceType.cosine)
      case "hamming" => DistanceMessage(DistanceMessage.DistanceType.hamming)
      case "jaccard" => DistanceMessage(DistanceMessage.DistanceType.jaccard)
      case "kullbackleibler" => DistanceMessage(DistanceMessage.DistanceType.kullbackleibler)
      case "chebyshev" => DistanceMessage(DistanceMessage.DistanceType.chebyshev)
      case "euclidean" => DistanceMessage(DistanceMessage.DistanceType.euclidean)
      case "squaredeuclidean" => DistanceMessage(DistanceMessage.DistanceType.squaredeuclidean)
      case "manhattan" => DistanceMessage(DistanceMessage.DistanceType.manhattan)
      case "minkowski" => DistanceMessage(DistanceMessage.DistanceType.minkowski)
      case "spannorm" => DistanceMessage(DistanceMessage.DistanceType.spannorm)
      case "modulo" => DistanceMessage(DistanceMessage.DistanceType.modulo)
      case "haversine" => DistanceMessage(DistanceMessage.DistanceType.haversine)
      case _ => DistanceMessage(DistanceMessage.DistanceType.minkowski, Map("norm" -> "1"))
    }
  }


  /**
    *
    * @param vec
    * @return
    */
  private[datastructures] def sparsify(vec: Seq[Float]) = {

    val ii = new ListBuffer[Int]()
    val vv = new ListBuffer[Float]()

    vec.zipWithIndex.foreach { x =>
      val v = x._1
      val i = x._2

      if (math.abs(v) > 1E-10) {
        ii.append(i)
        vv.append(v)
      }
    }

    (vv.toArray, ii.toArray, vec.size)
  }
}
