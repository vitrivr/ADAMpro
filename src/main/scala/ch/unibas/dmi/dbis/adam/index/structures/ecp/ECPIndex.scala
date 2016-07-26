package ch.unibas.dmi.dbis.adam.index.structures.ecp

import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature.FeatureVector
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.index.Index.{IndexName, IndexTypeName}
import ch.unibas.dmi.dbis.adam.index._
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.distance.Distance.Distance
import ch.unibas.dmi.dbis.adam.query.distance.DistanceFunction
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}


/**
  * adamtwo
  *
  * Ivan Giangreco
  * October 2015
  */
class ECPIndex(val indexname: IndexName, val entityname: EntityName, override private[index] var data: DataFrame, private[index] val metadata: ECPIndexMetaData)(@transient override implicit val ac: AdamContext)
  extends Index {

  override val indextypename: IndexTypeName = IndexTypes.ECPINDEX

  override val lossy: Boolean = true
  override val confidence = 0.5.toFloat

  /**
    *
    * @param data     rdd to scan
    * @param q        query vector
    * @param distance distance funciton
    * @param options  options to be passed to the index reader
    * @param k        number of elements to retrieve (of the k nearest neighbor search), possibly more than k elements are returned
    * @return a set of candidate tuple ids, possibly together with a tentative score (the number of tuples will be greater than k)
    */
  override def scan(data: DataFrame, q: FeatureVector, distance: DistanceFunction, options: Map[String, Any], k: Int): DataFrame = {
    log.debug("scanning eCP index " + indexname)

    //for every leader, check its distance to the query-vector, then sort by distance.
    val centroids = ac.sc.broadcast(metadata.leaders.map(l => {
      (l.id, metadata.distance(q, l.feature))
    }).sortBy(_._2))

    log.trace("centroids prepared")

    //iterate over all centroids until the result-count is over k
    import org.apache.spark.sql.functions.lit
    var results: DataFrame = null
    var i = 0
    var counter = 0
    do {
      //the distance returned here is the distance to the centroid, not the distance to the tuple
      val nns = data.filter(data(FieldNames.featureIndexColumnName) === centroids.value(i)._1).select(pk.name).withColumn(FieldNames.distanceColumnName, lit(centroids.value(i)._2).cast(DataTypes.FloatType))

      if (results != null) {
        results = results.unionAll(nns)
      } else {
        results = nns
      }
      counter += nns.count().toInt
      i += 1
    } while (i < centroids.value.length && counter < k)

    log.debug(options.toString())
    if (options.getOrElse("locality", "false").equals("false")) {
      results
    } else {
      val partInfo: RDD[(Int, Int)] = results.rdd.mapPartitionsWithIndex((idx, f) => {
        Iterator((idx, f.size))
      })

      log.debug("results number of partitions: " + results.rdd.getNumPartitions)
      log.debug("results partitioning info: ")
      val arr2 = partInfo.collect()
      for (i <- arr2) {
        log.debug(i.toString)
      }

      log.debug("Starting id lookup")
      val ids = results.map(f => f.getAs[Long](pk.name)).collect()
      log.debug("Collected ids")

      //TODO When doing Replication, handle first here
      val origins : Array[(Long, Int)] = ids.map(f => (f, findInPartition(f, data).first()))
      log.debug("Found tuples in Partitions")

      val counts: Map[Int, Array[(Long, Int)]] = origins.groupBy[Int]((f: (Long, Int)) => f._2)

      for (i <- counts) {
        log.debug("Partition " + i._1 + " contained: " + i._2.length + " Tuples")
      }

      val rdd: RDD[Row] = ac.sc.parallelize(origins.map(f => Row(f._1,f._2)))
      ac.sqlContext.createDataFrame(rdd,StructType(Seq(StructField(pk.name,DataTypes.LongType))))
      val originDF = ac.sqlContext.createDataFrame(rdd,  StructType(Seq(StructField(pk.name,DataTypes.LongType), StructField(FieldNames.provenanceColumnName,DataTypes.IntegerType))))
      log.debug("Results schema: "+results.schema.treeString)
      log.debug("origin Info Schema: "+originDF.schema.treeString)
      val temp: DataFrame = results.join(originDF, pk.name)
      log.debug("temp schema: "+temp.schema.treeString)
      log.debug("temp count: " + temp.count().toString)
      if(temp.count()>=1){
        log.debug("Example row: "+temp.first().toString())
      }
      temp
    }

  }

  def findInPartition(id: Long, data: DataFrame): RDD[Int] = {
    val res = data.rdd.mapPartitionsWithIndex((idx, it) => {
      if (it.exists(row => row.getAs[Long](pk.name) == id)) {
        Iterator(idx)
      } else {
        Iterator()
      }
    })
    res
  }

  override def isQueryConform(nnq: NearestNeighbourQuery): Boolean = true
}

object ECPIndex {
  def apply(indexname: IndexName, tablename: EntityName, data: DataFrame, meta: Any)(implicit ac: AdamContext): ECPIndex = {
    val indexMetaData = meta.asInstanceOf[ECPIndexMetaData]
    new ECPIndex(indexname, tablename, data, indexMetaData)
  }
}