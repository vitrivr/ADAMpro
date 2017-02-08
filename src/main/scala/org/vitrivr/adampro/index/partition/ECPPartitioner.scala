package org.vitrivr.adampro.index.partition

import org.vitrivr.adampro.config.AttributeNames
import org.vitrivr.adampro.datatypes.vector.Vector._
import org.vitrivr.adampro.entity.{Entity, EntityNameHolder}
import org.vitrivr.adampro.exception.GeneralAdamException
import org.vitrivr.adampro.index.structures.IndexTypes
import org.vitrivr.adampro.index.structures.ecp.ECPIndexMetaData
import org.vitrivr.adampro.index.{Index, IndexingTaskTuple}
import org.vitrivr.adampro.main.{AdamContext, SparkStartup}
import org.vitrivr.adampro.query.distance.DistanceFunction
import org.vitrivr.adampro.utils.Logging
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.util.random.Sampling
import org.vitrivr.adampro.entity.Entity.AttributeName
import org.vitrivr.adampro.index.Index.IndexName

/**
  * ADAMpar
  *
  * Silvan Heller
  * June 2016
  */
class ECPPartitioner(meta: ECPPartitionerMetaData, indexmeta: ECPIndexMetaData) extends Partitioner with Logging {

  override def numPartitions: Int = meta.nPart

  /**
    * Assigns a tuple to the closest partitioning leader
    */
  override def getPartition(key: Any): Int = {
    val leaderAssignment = key.asInstanceOf[Int]
    val leader = indexmeta.leaders.find(_.id == leaderAssignment).get
    val part = meta.leaders.sortBy(el => meta.distance(el.ap_indexable, leader.vector)).head.ap_id.asInstanceOf[Int]
    part
  }
}

object ECPPartitioner extends CustomPartitioner with Logging with Serializable {
  override def partitionerName: PartitionerChoice.Value = PartitionerChoice.ECP

  /**
    * This uses eCP on the eCP-leaders.
    * Improvements to be done: Use K-Means, partition size balancing, soft-assignment
    */
  def trainLeaders(indexmeta: ECPIndexMetaData, npart: Int)(implicit ac: AdamContext): Array[IndexingTaskTuple] = {
    val trainingSize = npart
    val fraction = Sampling.computeFractionForSampleSize(trainingSize, indexmeta.leaders.size, withReplacement = false)
    val leaders = ac.sc.parallelize(indexmeta.leaders)
    val traindata = leaders.sample(withReplacement = false, fraction = fraction).collect()
    traindata.take(npart).zipWithIndex.map(f => IndexingTaskTuple(f._2.toLong, f._1.vector))
  }

  /**
    * Repartition Data based on the eCP-Idea. Assigns each eCP-leader to a cluster which is chosen like in the eCP-Method
    * Data points are then assigned partitions based on their ecp-leaders. Distance comparison would be cleaner, but the ecp-index only stores ecp-leaders
    *
    * @param data        DataFrame you want to partition
    * @param attribute        Irrelevant here
    * @param indexName   Will be used to store partitioner information in the catalog
    * @param npartitions how many partitions shall be created
    * @return the partitioned DataFrame
    */
  override def apply(data: DataFrame, attribute: Option[AttributeName], indexName: Option[IndexName], npartitions: Int, options: Map[String, String] = Map[String, String]())(implicit ac: AdamContext): DataFrame = {
    import ac.spark.implicits._

    //loads the first ECPIndex
    val index = try {
      Entity.load(Index.load(indexName.get).get.entityname).get.indexes.find(f => f.get.indextypename == IndexTypes.ECPINDEX).get.get
    } catch {
      case e: java.util.NoSuchElementException => throw new GeneralAdamException("Repartitioning Failed because ECP Index was not created")
    }
    val joinDF = index.getData().get.withColumnRenamed(AttributeNames.featureIndexColumnName, AttributeNames.partitionKey)
    val joinedDF = data.join(joinDF, index.pk.name)
    log.debug("repartitioning ")

    val indexmeta = SparkStartup.catalogOperator.getIndexMeta(index.indexname).get.asInstanceOf[ECPIndexMetaData]
    val leaders = trainLeaders(indexmeta, npartitions)

    SparkStartup.catalogOperator.dropPartitioner(indexName.get)
    SparkStartup.catalogOperator.createPartitioner(indexName.get, npartitions, new ECPPartitionerMetaData(npartitions, leaders, indexmeta.distance), ECPPartitioner)
    //repartition
    val partitioner = new ECPPartitioner(new ECPPartitionerMetaData(npartitions, leaders, indexmeta.distance), indexmeta)
    val repartitioned: RDD[(Any, Row)] = joinedDF.map(r => (r.getAs[Any](AttributeNames.partitionKey), r)).rdd.partitionBy(partitioner)
    val reparRDD = repartitioned.mapPartitions((it) => {
      it.map(f => f._2)
    }, true)

    ac.sqlContext.createDataFrame(reparRDD, joinedDF.schema)
  }

  /** Returns the partitions to be queried for a given Feature vector
    * Simply compares to partition leaders
    * */
  override def getPartitions(q: MathVector, dropPercentage: Double, indexName: EntityNameHolder)(implicit ac: AdamContext): Seq[Int] = {
    val meta = SparkStartup.catalogOperator.getPartitionerMeta(indexName).get.asInstanceOf[ECPPartitionerMetaData]
    meta.leaders.sortBy(f => meta.distance(q, f.ap_indexable)).dropRight((meta.nPart * dropPercentage).toInt).map(_.ap_id.toString.toInt)
  }
}

case class ECPPartitionerMetaData(nPart: Int, leaders: Seq[IndexingTaskTuple], distance: DistanceFunction) extends Serializable {}
