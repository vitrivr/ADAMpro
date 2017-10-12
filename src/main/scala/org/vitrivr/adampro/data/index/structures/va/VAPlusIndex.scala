package org.vitrivr.adampro.data.index.structures.va


import org.apache.spark.sql.{DataFrame, Row}
import org.vitrivr.adampro.data.datatypes.vector.Vector
import org.vitrivr.adampro.data.datatypes.vector.Vector._
import org.vitrivr.adampro.query.tracker.QueryTracker
import org.vitrivr.adampro.data.index.Index._
import org.vitrivr.adampro.data.index.structures.IndexTypes
import org.vitrivr.adampro.process.SharedComponentContext
import org.vitrivr.adampro.query.distance.DistanceFunction
import org.apache.spark.ml.linalg.{DenseVector, Vectors}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * September 2016
  *
  * see H. Ferhatosmanoglu, E. Tuncel, D. Agrawal, A. El Abbadi (2006): High dimensional nearest neighbor searching. Information Systems.
  */
class VAPlusIndex(override val indexname: IndexName)(@transient override implicit val ac: SharedComponentContext) extends VAIndex(indexname)(ac) {
  override lazy val indextypename: IndexTypeName = IndexTypes.VAPLUSINDEX

  override lazy val lossy: Boolean = meta.asInstanceOf[VAPlusIndexMetaData].approximate
  override lazy val confidence: Float = if (meta.asInstanceOf[VAPlusIndexMetaData].approximate) {
    0.9.toFloat
  } else {
    1.0.toFloat
  }
  override lazy val score: Float = if (meta.asInstanceOf[VAPlusIndexMetaData].approximate) {
    0.9.toFloat
  } else {
    1.0.toFloat
  }

  override def scan(data: DataFrame, q: MathVector, distance: DistanceFunction, options: Map[String, String], k: Int)(tracker : QueryTracker): DataFrame = {
    val queries = ac.spark.createDataFrame(Seq(Tuple1(Vectors.dense(q.toArray.map(_.toDouble))))).toDF("queries")

    val adjustedQuery: Array[Row] = meta.asInstanceOf[VAPlusIndexMetaData].pca.setInputCol("queries").setOutputCol("pcaQueries").transform(queries).collect()
    super.scan(data, new DenseMathVector(adjustedQuery.head.getAs[DenseVector](0).values.map(Vector.conv_double2vb(_))), distance, options, k)(tracker)
  }
}
