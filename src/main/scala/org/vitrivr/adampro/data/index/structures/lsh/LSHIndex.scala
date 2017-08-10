package org.vitrivr.adampro.data.index.structures.lsh

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, udf}
import org.vitrivr.adampro.config.AttributeNames
import org.vitrivr.adampro.data.datatypes.bitstring.BitString
import org.vitrivr.adampro.data.datatypes.vector.Vector._
import org.vitrivr.adampro.data.datatypes.vector.MovableFeature
import org.vitrivr.adampro.query.tracker.QueryTracker
import org.vitrivr.adampro.data.index.Index
import org.vitrivr.adampro.data.index.Index.{IndexName, IndexTypeName}
import org.vitrivr.adampro.data.index.structures.IndexTypes
import org.vitrivr.adampro.data.index.structures.lsh.signature.LSHSignatureGenerator
import org.vitrivr.adampro.process.SharedComponentContext
import org.vitrivr.adampro.query.distance.{Distance, DistanceFunction}
import org.vitrivr.adampro.query.query.RankingQuery


/**
  * adamtwo
  *
  * Ivan Giangreco
  * August 2015
  */
class LSHIndex(override val indexname: IndexName)(@transient override implicit val ac: SharedComponentContext)
  extends Index(indexname)(ac) {

  override val indextypename: IndexTypeName = IndexTypes.LSHINDEX
  override val lossy: Boolean = true
  override val confidence = 0.5.toFloat

  val meta = metadata.get.asInstanceOf[LSHIndexMetaData]


  override def scan(data: DataFrame, q: MathVector, distance: DistanceFunction, options: Map[String, String], k: Int)(tracker : QueryTracker): DataFrame = {
    log.trace("scanning LSH index")

    val nAdditionalQueries = options.getOrElse("numOfQ", "3").toInt

    val signatureGeneratorBc = ac.sc.broadcast( new LSHSignatureGenerator(meta.ghashf, meta.m))
    tracker.addBroadcast(signatureGeneratorBc)

    import MovableFeature.conv_math2mov
    val originalQuery = signatureGeneratorBc.value.toBuckets(q)
    //move the query around by the precomuted radius
    //TODO: possibly adjust weight of computed queries vs. true query
    val queries = List.fill(nAdditionalQueries)((1.0, signatureGeneratorBc.value.toBuckets(q.move(meta.radius)))) ::: List((1.0, originalQuery))

    val nqueries = queries.length
    val flattenedQueriesBc = ac.sc.broadcast(queries.map(_._2).flatten.toArray)
    tracker.addBroadcast(flattenedQueriesBc)

    val containsUDF = udf((c: Array[Byte]) => {
      val buckets = signatureGeneratorBc.value.toBuckets(BitString.fromByteArray(c))

      var found = false

      var i = 0
      var j = 0

      while (i < buckets.length && !found) {
        j = 0
        while(j < nqueries){
          if(flattenedQueriesBc.value(i * nqueries + j) == buckets(i)){
            found = true
          }

          j += 1
        }

        i += 1
      }

      found
    })

    val res = data
      .filter(containsUDF(col(AttributeNames.featureIndexColumnName)))
      .withColumn(AttributeNames.distanceColumnName, lit(Distance.zeroValue))

    res
  }

  override def isQueryConform(nnq: RankingQuery): Boolean = {
    nnq.distance.equals(meta.distance)
  }
}