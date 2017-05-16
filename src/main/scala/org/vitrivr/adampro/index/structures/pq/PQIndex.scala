package org.vitrivr.adampro.index.structures.pq

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.DataTypes
import org.vitrivr.adampro.config.AttributeNames
import org.vitrivr.adampro.datatypes.vector.Vector
import org.vitrivr.adampro.datatypes.vector.Vector._
import org.vitrivr.adampro.index.Index
import org.vitrivr.adampro.index.Index.{IndexName, IndexTypeName}
import org.vitrivr.adampro.index.structures.IndexTypes
import org.vitrivr.adampro.main.AdamContext
import org.vitrivr.adampro.query.distance.{Distance, DistanceFunction, MinkowskiDistance}
import org.vitrivr.adampro.query.query.NearestNeighbourQuery
import org.vitrivr.adampro.datatypes.vector.Vector._
import org.vitrivr.adampro.helpers.tracker.OperationTracker

/**
  * adampro
  *
  * Ivan Giangreco
  * April 2016
  */
class PQIndex(override val indexname: IndexName)(@transient override implicit val ac : AdamContext)
  extends Index(indexname)(ac) {

  override val indextypename: IndexTypeName = IndexTypes.PQINDEX
  override val lossy: Boolean = true
  override val confidence: Float = 0.5.toFloat

  val meta = metadata.get.asInstanceOf[PQIndexMetaData]

  override def scan(data : DataFrame, q : MathVector, distance : DistanceFunction, options : Map[String, String], k : Int)(tracker : OperationTracker): DataFrame = {
    log.debug("scanning PQ index " + indexname)

    //precompute distance
    val distancesBc = ac.sc.broadcast(q.toArray
      .grouped(math.max(1, q.size / meta.nsq)).toSeq
      .zipWithIndex
      .map { case (split, idx) =>
        val qsub = split
        meta.models(idx).clusterCenters.map(center => {
          distance(Vector.conv_draw2vec(center.toArray.map(Vector.conv_double2vb(_))), Vector.conv_draw2vec(qsub))
        }).toIndexedSeq
      }.toIndexedSeq)
    tracker.addBroadcast(distancesBc)

    import org.apache.spark.sql.functions.udf
    val distUDF = udf((c: Seq[Byte]) => {
      var i : Int = 0
      var sum : VectorBase = 0
      //sum up distance of each part by choosing the right cluster
      while(i < c.length){
        sum += Vector.conv_double2vb(distancesBc.value(i)(c(i)))
        i += 1
      }
      sum
    })

    val res = data
      .withColumn(AttributeNames.distanceColumnName, distUDF(data(AttributeNames.featureIndexColumnName)).cast(Distance.SparkDistance))
      .sort(AttributeNames.distanceColumnName)
      .limit(k)

    res
  }

  override def isQueryConform(nnq: NearestNeighbourQuery): Boolean = {
    if (nnq.distance.isInstanceOf[MinkowskiDistance]) {
      return true
    }

    false
  }
}