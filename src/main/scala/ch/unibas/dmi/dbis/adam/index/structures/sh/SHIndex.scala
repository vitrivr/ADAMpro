package ch.unibas.dmi.dbis.adam.index.structures.sh

import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.datatypes.bitString.BitString
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.datatypes.feature.MovableFeature
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.index.Index.{IndexName, IndexTypeName}
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.Result
import ch.unibas.dmi.dbis.adam.query.distance.{DistanceFunction, MinkowskiDistance}
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery
import org.apache.spark.sql.{Row, DataFrame}


/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
class SHIndex(val indexname: IndexName, val entityname: EntityName, override private[index] var data : DataFrame, private[index] val metadata: SHIndexMetaData)(@transient override implicit val ac : AdamContext)
  extends Index {

  override val indextypename: IndexTypeName = IndexTypes.SHINDEX

  override val lossy: Boolean = true
  override val confidence = 0.toFloat

  /**
    *
    * @param data     rdd to scan
    * @param q        query vector
    * @param distance distance funciton
    * @param options  options to be passed to the index reader
    * @param k        number of elements to retrieve (of the k nearest neighbor search), possibly more than k elements are returned
    * @return a set of candidate tuple ids, possibly together with a tentative score (the number of tuples will be greater than k)
    */
  override def scan(data : DataFrame, q : FeatureVector, distance : DistanceFunction, options : Map[String, Any], k : Int): DataFrame = {
    log.debug("scanning SH index " + indexname)

    val numOfQueries = options.getOrElse("numOfQ", "3").asInstanceOf[String].toInt

    import MovableFeature.conv_feature2MovableFeature
    val originalQuery = SHUtils.hashFeature(q, metadata)
    val queries = ac.sc.broadcast(List.fill(numOfQueries)(SHUtils.hashFeature(q.move(metadata.radius), metadata)) ::: List(originalQuery))

    val maxScore : Float = originalQuery.intersectionCount(originalQuery) * numOfQueries

    import org.apache.spark.sql.functions.udf
    val distUDF = udf((c: BitString[_]) => {
      var i = 0
      var score = 0
      while (i < queries.value.length) {
        val query = queries.value(i)
        score += c.intersectionCount(query)
        i += 1
      }

      score
    })

    val rddResults = data
      .withColumn(FieldNames.distanceColumnName, distUDF(data(FieldNames.featureIndexColumnName)))
      .mapPartitions { items =>
        val handler = new SHResultHandler(k)

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


object SHIndex {
  def apply(indexname: IndexName, tablename: EntityName, data: DataFrame, meta: Any)(implicit ac : AdamContext): SHIndex = {
    val indexMetaData = meta.asInstanceOf[SHIndexMetaData]
    new SHIndex(indexname, tablename, data, indexMetaData)
  }
}