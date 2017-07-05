package org.vitrivr.adampro.query.distance

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.vitrivr.adampro.data.datatypes.vector.Vector
import org.vitrivr.adampro.data.datatypes.vector.Vector._
import org.vitrivr.adampro.query.query.RankingQuery
import org.vitrivr.adampro.utils.Logging

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * August 2015
  */
object Distance extends Logging {
  type Distance = Double
  val SparkDistance = DoubleType
  val maxValue : Distance = Double.MaxValue
  val zeroValue : Distance = 0.toDouble

  /**
    *
    */
  val denseVectorDistUDF = (nnq : RankingQuery, q : Broadcast[MathVector], w : Broadcast[Option[MathVector]]) => udf((c: DenseSparkVector) => {
    try {
      if (c != null) {
        nnq.distance(q.value, Vector.conv_dspark2vec(c), w.value)
      } else {
        maxValue
      }
    } catch {
      case e: Exception =>
        log.error("error when computing distance", e)
        maxValue
    }
  })

  /**
    *
    */
  val sparseVectorDistUDF = (nnq : RankingQuery, q : Broadcast[MathVector], w : Broadcast[Option[MathVector]]) => udf((c: SparseSparkVector) => {
    try {
      if (c != null) {
        nnq.distance(q.value, Vector.conv_sspark2vec(c), w.value)
      } else {
        maxValue
      }
    } catch {
      case e: Exception =>
        log.error("error when computing distance", e)
        maxValue
    }
  })
}
