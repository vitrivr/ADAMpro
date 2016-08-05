package ch.unibas.dmi.dbis.adam.helpers.partition

import ch.unibas.dmi.dbis.adam.catalog.CatalogOperator
import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.entity.{Entity, EntityNameHolder}
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.index.structures.pq.PQIndexMetaData
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.utils.Logging
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

/**
  * Created by silvanheller on 05.08.16.
  */
class PQPartitioner(metaData: PQPartitionerMetaData) extends Partitioner with Logging {
  override def numPartitions: Int = metaData.getNoPartitions

  override def getPartition(key: Any): Int = {
    throw new UnsupportedOperationException
  }
}

object PQPartitioner extends ADAMPartitioner with Logging with Serializable {
  override def partitionerName = PartitionerChoice.PQ

  /** Returns the partitions to be queried for a given feature vector */
  override def getPartitions(q: FeatureVector, dropPercentage: Double, indexName: EntityNameHolder)(implicit ac: AdamContext): Seq[Int] = {
    val meta = CatalogOperator.getPartitionerMeta(indexName).get.asInstanceOf[PQIndexMetaData]
    throw new UnsupportedOperationException
  }

  def train(df: DataFrame, nPart: Int, indexmeta: PQIndexMetaData) : PQPartitionerMetaData = {
    throw new UnsupportedOperationException
  }


  override def apply(data: DataFrame, cols: Option[Seq[String]], indexName: Option[EntityNameHolder], nPartitions: Int)(implicit ac: AdamContext): DataFrame = {
    //This line causes you to load the data from the first index that is found which matches the type
    val index = Entity.load(Index.load(indexName.get).get.entityname).get.indexes.find(f => f.get.indextypename == IndexTypes.SHINDEX).get.get
    val indexmeta = CatalogOperator.getIndexMeta(index.indexname).get.asInstanceOf[PQIndexMetaData]
    val joinDF = index.getData.withColumnRenamed(FieldNames.featureIndexColumnName, FieldNames.partitionKey)
    val joinedDF = data.join(joinDF, index.pk.name)

    val meta = train(joinDF, nPartitions, indexmeta)

    //drop old partitioner, create new
    CatalogOperator.dropPartitioner(indexName.get).get
    CatalogOperator.createPartitioner(indexName.get, nPartitions, meta, PQPartitioner)

    //repartition
    val partitioner = new PQPartitioner(meta)
    val repartitioned: RDD[(Any, Row)] = joinedDF.map(r => (r.getAs[Any](FieldNames.partitionKey), r)).partitionBy(partitioner)
    val reparRDD = repartitioned.mapPartitions((it) => {
      it.map(f => f._2)
    }, true)

    ac.sqlContext.createDataFrame(reparRDD, joinedDF.schema)
  }

}

class PQPartitionerMetaData(nPart: Int) extends Serializable {
  def getNoPartitions: Int = nPart
}
