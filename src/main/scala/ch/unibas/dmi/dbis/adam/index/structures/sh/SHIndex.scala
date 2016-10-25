package ch.unibas.dmi.dbis.adam.index.structures.sh

import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.datatypes.bitString.BitString
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.datatypes.feature.MovableFeature
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.index.Index.{IndexName, IndexTypeName}
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.Result
import ch.unibas.dmi.dbis.adam.query.distance.{DistanceFunction, MinkowskiDistance}
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery
import org.apache.spark.sql.{DataFrame, Row}


/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
class SHIndex(override val indexname: IndexName)(@transient override implicit val ac : AdamContext)
  extends Index(indexname) {

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
  override def scan(data : DataFrame, q : FeatureVector, distance : DistanceFunction, options : Map[String, String], k : Int): DataFrame = {
    log.debug("scanning SH index " + indexname)

    val numOfQueries = options.getOrElse("numOfQ", "3").toInt

    import MovableFeature.conv_feature2MovableFeature
    val originalQuery = SHUtils.hashFeature(q, meta)
    //move the query around by the precomuted radius
    //TODO: possibly adjust weight of computed queries vs. true query
    val queries = ac.sc.broadcast(List.fill(numOfQueries)((1.0, SHUtils.hashFeature(q.move(meta.radius), meta))) ::: List((1.0, originalQuery)))

    import org.apache.spark.sql.functions.udf
    val distUDF = udf((c: BitString[_]) => {
      var i = 0
      var score = 0
      while (i < queries.value.length) {
        val weight = queries.value(i)._1
        val query = queries.value(i)._2
        score += c.hammingDistance(query) //hamming distance
        i += 1
      }

      score
    })

    val rddResults = data
      .withColumn(FieldNames.distanceColumnName, distUDF(data(FieldNames.featureIndexColumnName)))
      .mapPartitions { items =>
        val handler = new SHResultHandler(k)  //use handler to take closest n elements
        //(using this handler is necessary here, as if the closest element has distance 5, we want all closes elements with distance 5;
        //the methods provided by Spark (e.g. take) do not allow this

        items.foreach(item => {
          handler.offer(item, this.pk.name)
        })

        handler.results.map(x => Row(x.tid, x.score.toFloat)).iterator
      }

    ac.sqlContext.createDataFrame(rddResults,  Result.resultSchema(pk))
  }

  override def isQueryConform(nnq: NearestNeighbourQuery): Boolean = {
    //is this check correct?
    if(nnq.distance.isInstanceOf[MinkowskiDistance]){
      return true
    }

    false
  }
}