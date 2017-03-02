package org.vitrivr.adampro.index.structures.lsh

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.vitrivr.adampro.config.AttributeNames
import org.vitrivr.adampro.datatypes.bitstring.BitString
import org.vitrivr.adampro.datatypes.vector.Vector._
import org.vitrivr.adampro.datatypes.vector.MovableFeature
import org.vitrivr.adampro.index.Index
import org.vitrivr.adampro.index.Index.{IndexName, IndexTypeName}
import org.vitrivr.adampro.index.structures.IndexTypes
import org.vitrivr.adampro.index.structures.lsh.signature.LSHSignatureGenerator
import org.vitrivr.adampro.main.AdamContext
import org.vitrivr.adampro.query.distance.DistanceFunction
import org.vitrivr.adampro.query.query.NearestNeighbourQuery


/**
  * adamtwo
  *
  * Ivan Giangreco
  * August 2015
  */
class LSHIndex(override val indexname: IndexName)(@transient override implicit val ac: AdamContext)
  extends Index(indexname)(ac) {

  override val indextypename: IndexTypeName = IndexTypes.LSHINDEX
  override val lossy: Boolean = true
  override val confidence = 0.5.toFloat

  val meta = metadata.get.asInstanceOf[LSHIndexMetaData]


  override def scan(data: DataFrame, q: MathVector, distance: DistanceFunction, options: Map[String, String], k: Int): DataFrame = {
    log.debug("scanning LSH index " + indexname)

    val numOfQueries = options.getOrElse("numOfQ", "3").toInt

    val signatureGeneratorBc = ac.sc.broadcast( new LSHSignatureGenerator(meta.hashTables, meta.m))

    import MovableFeature.conv_math2mov
    val originalQuery = signatureGeneratorBc.value.toBuckets(q)
    //move the query around by the precomuted radius
    //TODO: possibly adjust weight of computed queries vs. true query
    val queriesBc = ac.sc.broadcast(List.fill(numOfQueries)((1.0, signatureGeneratorBc.value.toBuckets(q.move(meta.radius)))) ::: List((1.0, originalQuery)))

    import org.apache.spark.sql.functions.udf
    val distUDF = udf((c: Array[Byte]) => {
      var i = 0
      var score = 0
      val buckets = signatureGeneratorBc.value.toBuckets(BitString.fromByteArray(c))

      while (i < queriesBc.value.length) {
        var j = 0
        var sum = 0

        val weight = queriesBc.value(i)._1
        val query = queriesBc.value(i)._2

        while(j < buckets.length){
          if(buckets(j) == query(j)){
            sum += 1
          }

          j += 1
        }

        score += sum
        i += 1
      }

      score
    })

    val res = data
      .withColumn(AttributeNames.distanceColumnName, distUDF(data(AttributeNames.featureIndexColumnName)))
      .filter(col(AttributeNames.distanceColumnName) > 0)

    res
  }

  override def isQueryConform(nnq: NearestNeighbourQuery): Boolean = {
    nnq.distance.equals(meta.distance)
  }
}