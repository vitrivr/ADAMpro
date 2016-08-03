package ch.unibas.dmi.dbis.adam.helpers.partition

import ch.unibas.dmi.dbis.adam.catalog.CatalogOperator
import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature.FeatureVector
import ch.unibas.dmi.dbis.adam.entity.{Entity, EntityNameHolder}
import ch.unibas.dmi.dbis.adam.index.{Index, IndexGenerator, IndexingTaskTuple}
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.index.structures.ecp.ECPIndexMetaData
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.distance.DistanceFunction
import ch.unibas.dmi.dbis.adam.utils.Logging
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.util.random.Sampling

/**
  * Created by silvanheller on 03.08.16.
  */
class ECPPartitioner(meta: ECPPartitionerMetaData, indexmeta: ECPIndexMetaData) extends Partitioner with Logging {

  override def numPartitions: Int = meta.getNoPart

  //TODO Verify if this works
  override def getPartition(key: Any): Int = {
    val leaderAssignment = key.asInstanceOf[Int]
    log.debug("Parsed leader assignment: "+leaderAssignment)
    val leader = indexmeta.leaders.find(_.id==leaderAssignment).get
    val part = meta.getLeaders.sortBy(el => meta.getDistanceFunction(el.feature,leader.feature)).head.id.asInstanceOf[Int]
    log.debug("Assigned Partition: "+part)
    part
  }
}

object ECPPartitioner extends ADAMPartitioner with Logging with Serializable{

  override def partitionerName: PartitionerChoice.Value = PartitionerChoice.ECP

  /**
    * This uses eCP on the eCP-leaders.
    * TODO Maybe use multi-layered eCP? -> How would we store that sensibly
    */
  def sampleLeaders(indexmeta: ECPIndexMetaData, nPart: Int)(implicit ac: AdamContext) : Array[IndexingTaskTuple[Int]] = {
    val trainingSize = nPart
    val fraction = Sampling.computeFractionForSampleSize(trainingSize, indexmeta.leaders.size, withReplacement = false)
    val leaders = ac.sc.parallelize(indexmeta.leaders)
    val l = leaders.sample(false, fraction).collect.take(nPart).zipWithIndex.map(f => IndexingTaskTuple(f._2,f._1.feature))
    log.debug("Chosen leadrs: "+l.mkString(", "))
    l
  }

  /**
    * Repartitions Data based on the eCP-Idea. Assigns each eCP-leader to a cluster which is chosen like in the eCP-Method
    * Datapoints are then assigned partitions based on their ecp-leaders. Distance comparison would be cleaner, but the ecp-index only stores ecp-leaders
    *
    * @param data DataFrame you want to partition
    * @param cols Irrelevant here
    * @param indexName Will be used to store partitioner information in the catalog
    * @param nPartitions how many partitions shall be created
    * @return the partitioned DataFrame
    */
  override def apply(data: DataFrame, cols: Option[Seq[String]], indexName: Option[EntityNameHolder], nPartitions: Int)(implicit ac: AdamContext): DataFrame = {
    CatalogOperator.dropPartitioner(indexName.get)

    //loads the first ECPIndex
    val index = Entity.load(Index.load(indexName.get).get.entityname).get.indexes.find(f => f.get.indextypename == IndexTypes.ECPINDEX).get.get
    val joinDF = index.getData.withColumnRenamed(FieldNames.featureIndexColumnName, FieldNames.partitionKey)
    joinDF.show(2)
    val joinedDF = data.join(joinDF, index.pk.name)
    joinedDF.show(2)
    log.debug("repartitioning ")

    val indexmeta = CatalogOperator.getIndexMeta(index.indexname).get.asInstanceOf[ECPIndexMetaData]
    val leaders = sampleLeaders(indexmeta, nPartitions)
    CatalogOperator.createPartitioner(indexName.get,nPartitions,new ECPPartitionerMetaData(nPartitions,leaders, indexmeta.distance),ECPPartitioner)

    //repartition
    val partitioner = new ECPPartitioner(new ECPPartitionerMetaData(nPartitions,leaders, indexmeta.distance), indexmeta)
    val repartitioned: RDD[(Any, Row)] = joinedDF.map(r => (r.getAs[Any](FieldNames.partitionKey), r)).partitionBy(partitioner)
    val reparRDD = repartitioned.mapPartitions((it) => {
      it.map(f => f._2)
    }, true)
    val ret = ac.sqlContext.createDataFrame(reparRDD, joinedDF.schema)
    log.debug("repartitioned")
    ret.show(2)
    ret
  }

  /** Returns the partitions to be queried for a given Featurevector */
  override def getPartitions(q: FeatureVector, dropPercentage: Double, indexName: EntityNameHolder)(implicit ac: AdamContext): Seq[Int] = {
    val meta = CatalogOperator.getPartitionerMeta(indexName).get.asInstanceOf[ECPPartitionerMetaData]
    meta.getLeaders.sortBy(f => meta.getDistanceFunction(q, f.feature)).dropRight((meta.getNoPart*dropPercentage).toInt).map(_.id.toString.toInt)
  }
}

class ECPPartitionerMetaData(nPart: Int, leaders: Seq[IndexingTaskTuple[_]], distance: DistanceFunction) extends Serializable {
  def getNoPart: Int = nPart
  def getLeaders :Seq[IndexingTaskTuple[_]] = leaders
  def getDistanceFunction: DistanceFunction = distance
}
