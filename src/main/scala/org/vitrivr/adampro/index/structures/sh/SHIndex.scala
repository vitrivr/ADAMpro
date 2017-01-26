package org.vitrivr.adampro.index.structures.sh

import org.apache.spark.sql.{DataFrame, Row}
import org.vitrivr.adampro.config.AttributeNames
import org.vitrivr.adampro.datatypes.bitstring.BitString
import org.vitrivr.adampro.datatypes.vector.Vector._
import org.vitrivr.adampro.datatypes.vector.MovableFeature
import org.vitrivr.adampro.index.{ResultElement, Index}
import org.vitrivr.adampro.index.Index.{IndexName, IndexTypeName}
import org.vitrivr.adampro.index.structures.IndexTypes
import org.vitrivr.adampro.main.AdamContext
import org.vitrivr.adampro.query.distance.{DistanceFunction, MinkowskiDistance}
import org.vitrivr.adampro.query.query.NearestNeighbourQuery


/**
  * adamtwo
  *
  * Ivan Giangreco
  * August 2015
  */
class SHIndex(override val indexname: IndexName)(@transient override implicit val ac: AdamContext)
  extends Index(indexname)(ac) {

  override val indextypename: IndexTypeName = IndexTypes.SHINDEX
  override val lossy: Boolean = true
  override val confidence = 0.5.toFloat

  val meta = metadata.get.asInstanceOf[SHIndexMetaData]

  /**
    *
    * @param data     rdd to scan
    * @param q        query vector
    * @param distance distance funciton
    * @param options  options to be passed to the index reader
    * @param k        number of elements to retrieve (of the k nearest neighbor search), possibly more than k elements are returned
    * @return a set of candidate tuple ids, possibly together with a tentative score (the number of tuples will be greater than k)
    */
  override def scan(data: DataFrame, q: MathVector, distance: DistanceFunction, options: Map[String, String], k: Int): DataFrame = {
    log.debug("scanning SH index " + indexname)

    val numOfQueries = options.getOrElse("numOfQ", "3").toInt

    import MovableFeature.conv_math2mov
    val originalQuery = SHUtils.hashFeature(q, meta)
    //move the query around by the precomuted radius
    //TODO: possibly adjust weight of computed queries vs. true query
    val queries = ac.sc.broadcast(List.fill(numOfQueries)((1.0, SHUtils.hashFeature(q.move(meta.radius), meta))) ::: List((1.0, originalQuery)))

    import org.apache.spark.sql.functions.udf
    val distUDF = udf((c: Array[Byte]) => {
      var i = 0
      var score = 0
      while (i < queries.value.length) {
        val weight = queries.value(i)._1
        val query = queries.value(i)._2
        score += BitString.fromByteArray(c).hammingDistance(query) //hamming distance
        i += 1
      }

      score
    })

    import ac.spark.implicits._
    data
      .withColumn(AttributeNames.distanceColumnName, distUDF(data(AttributeNames.featureIndexColumnName)))
      .mapPartitions { items =>
        val handler = new SHResultHandler(k) //use handler to take closest n elements
        //(using this handler is necessary here, as if the closest element has distance 5, we want all closes elements with distance 5;
        //the methods provided by Spark (e.g. take) do not allow this

        items.foreach(item => {
          handler.offer(item, this.pk.name)
        })

        handler.results.map(x => ResultElement(x.ap_id, x.ap_score)).iterator
      }.toDF()
  }

  override def isQueryConform(nnq: NearestNeighbourQuery): Boolean = {
    //is this check correct?
    if (nnq.distance.isInstanceOf[MinkowskiDistance]) {
      return true
    }

    false
  }
}