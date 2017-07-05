package org.vitrivr.adampro.index.structures.mi

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame}
import org.vitrivr.adampro.config.AttributeNames
import org.vitrivr.adampro.datatypes.bitstring.BitString
import org.vitrivr.adampro.datatypes.vector.Vector._
import org.vitrivr.adampro.helpers.tracker.OperationTracker
import org.vitrivr.adampro.index.Index
import org.vitrivr.adampro.index.Index._
import org.vitrivr.adampro.index.structures.IndexTypes
import org.vitrivr.adampro.main.SharedComponentContext
import org.vitrivr.adampro.query.distance.{Distance, DistanceFunction}
import org.vitrivr.adampro.query.query.NearestNeighbourQuery

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2016
  */
@Experimental class MIIndex(override val indexname: IndexName)(@transient override implicit val ac: SharedComponentContext)
  extends Index(indexname)(ac) {

  override val indextypename: IndexTypeName = IndexTypes.MIINDEX
  override val lossy: Boolean = true
  override val confidence = 0.5.toFloat

  val meta = metadata.get.asInstanceOf[MIIndexMetaData]


  override def scan(data: DataFrame, q: MathVector, distance: DistanceFunction, options: Map[String, String], k: Int)(tracker : OperationTracker): DataFrame = {
    log.debug("scanning MI index " + indexname)

    val ki = meta.ki
    //ks is the number of closest reference points to consider
    val ks = options.mapValues(_.toInt).getOrElse("ks", meta.ks)
    assert(ks <= ki)

    val signatureGeneratorBc = ac.sc.broadcast( new MISignatureGenerator(meta.ki, meta.refs.length))
    tracker.addBroadcast(signatureGeneratorBc)

    val max_pos_diff = ki + 1

    //take closest ks reference points
    val qrefs = meta.refs.sortBy(ref => distance(q, ref.ap_indexable)).take(ks).map(_.ap_id).zipWithIndex
    val qrefsBc = ac.sc.broadcast(qrefs)
    tracker.addBroadcast(qrefsBc)

    log.trace("reference points prepared")

    val distUDF = udf((c: Array[Byte]) => {
      val refs = signatureGeneratorBc.value.toBuckets(BitString.fromByteArray(c)).zipWithIndex.map(x => x._1 -> x._2).toMap

      qrefsBc.value.map{ qref =>
       refs.mapValues(x =>   math.abs(x - qref._2)).getOrElse(qref._1.toInt, max_pos_diff)
      }.sum
    })

    val res = data
      .withColumn(AttributeNames.distanceColumnName, distUDF(data(AttributeNames.featureIndexColumnName)).cast(Distance.SparkDistance))
      .orderBy(AttributeNames.distanceColumnName)
      .limit(k)

    res
  }


  override def isQueryConform(nnq: NearestNeighbourQuery): Boolean = true
}