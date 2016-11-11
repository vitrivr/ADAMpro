package org.vitrivr.adampro.index.structures.lsh

import org.vitrivr.adampro.config.FieldNames
import org.vitrivr.adampro.datatypes.bitString.BitString
import org.vitrivr.adampro.datatypes.feature.Feature._
import org.vitrivr.adampro.datatypes.feature.MovableFeature
import org.vitrivr.adampro.index.Index
import org.vitrivr.adampro.index.Index.{IndexName, IndexTypeName}
import org.vitrivr.adampro.index.structures.IndexTypes
import org.vitrivr.adampro.index.structures.lsh.signature.LSHSignatureGenerator
import org.vitrivr.adampro.main.AdamContext
import org.vitrivr.adampro.query.distance.DistanceFunction
import org.vitrivr.adampro.query.query.NearestNeighbourQuery
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col


/**
  * adamtwo
  *
  * Ivan Giangreco
  * August 2015
  */
class LSHIndex(override val indexname: IndexName)(@transient override implicit val ac: AdamContext)
  extends Index(indexname) {

  override val indextypename: IndexTypeName = IndexTypes.LSHINDEX
  override val lossy: Boolean = true
  override val confidence = 0.5.toFloat

  val meta = metadata.get.asInstanceOf[LSHIndexMetaData]


  override def scan(data: DataFrame, q: FeatureVector, distance: DistanceFunction, options: Map[String, String], k: Int): DataFrame = {
    log.debug("scanning LSH index " + indexname)

    val numOfQueries = options.getOrElse("numOfQ", "3").toInt

    val signatureGenerator = ac.sc.broadcast( new LSHSignatureGenerator(meta.hashTables, meta.m))

    import MovableFeature.conv_feature2MovableFeature
    val originalQuery = signatureGenerator.value.toBuckets(q)
    //move the query around by the precomuted radius
    //TODO: possibly adjust weight of computed queries vs. true query
    val queries = ac.sc.broadcast(List.fill(numOfQueries)((1.0, signatureGenerator.value.toBuckets(q.move(meta.radius)))) ::: List((1.0, originalQuery)))

    import org.apache.spark.sql.functions.udf
    val distUDF = udf((c: BitString[_]) => {
      var i = 0
      var score = 0
      val buckets = signatureGenerator.value.toBuckets(c)

      while (i < queries.value.length) {
        var j = 0
        var sum = 0

        val weight = queries.value(i)._1
        val query = queries.value(i)._2

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


    data
      .withColumn(FieldNames.distanceColumnName, distUDF(data(FieldNames.featureIndexColumnName)))
      .filter(col(FieldNames.distanceColumnName) > 0)
  }

  override def isQueryConform(nnq: NearestNeighbourQuery): Boolean = {
    nnq.distance.equals(meta.distance)
  }
}