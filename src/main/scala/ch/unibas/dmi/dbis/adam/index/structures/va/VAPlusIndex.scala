package ch.unibas.dmi.dbis.adam.index.structures.va

import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.datatypes.feature.FeatureVectorWrapper
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.distance.DistanceFunction
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

  override def scan(data: DataFrame, q: FeatureVector, distance: DistanceFunction, options: Map[String, String], k: Int): DataFrame = {
    val adjustedQuery = new FeatureVectorWrapper(meta.asInstanceOf[VAPlusIndexMetaData].pca.transform(Vectors.dense(q.toArray.map(_.toDouble))).toArray.map(_.toFloat))
    super.scan(data, adjustedQuery.vector, distance, options, k)
  }
}
