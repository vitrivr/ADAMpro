package org.vitrivr.adampro.index.structures.va

import org.vitrivr.adampro.datatypes.feature.Feature._
import org.vitrivr.adampro.datatypes.feature.FeatureVectorWrapper
import org.vitrivr.adampro.index.Index._
import org.vitrivr.adampro.index.structures.IndexTypes
import org.vitrivr.adampro.main.AdamContext
import org.vitrivr.adampro.query.distance.DistanceFunction
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.DataFrame

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * September 2016
  *
  * see H. Ferhatosmanoglu, E. Tuncel, D. Agrawal, A. El Abbadi (2006): High dimensional nearest neighbor searching. Information Systems.
  */
class VAPlusIndex(override val indexname: IndexName)(@transient override implicit val ac: AdamContext) extends VAIndex(indexname) {
  override lazy val indextypename: IndexTypeName = IndexTypes.VAPLUSINDEX

  override lazy val lossy: Boolean = meta.asInstanceOf[VAPlusIndexMetaData].approximate
  override lazy val confidence : Float = if(meta.asInstanceOf[VAPlusIndexMetaData].approximate){0.9} else {1.0}
  override lazy val score : Float= if(meta.asInstanceOf[VAPlusIndexMetaData].approximate){0.9} else {1.0}

  override def scan(data: DataFrame, q: FeatureVector, distance: DistanceFunction, options: Map[String, String], k: Int): DataFrame = {
    val adjustedQuery = new FeatureVectorWrapper(meta.asInstanceOf[VAPlusIndexMetaData].pca.transform(Vectors.dense(q.toArray.map(_.toDouble))).toArray.map(_.toFloat))
    super.scan(data, adjustedQuery.vector, distance, options, k)
  }
}
