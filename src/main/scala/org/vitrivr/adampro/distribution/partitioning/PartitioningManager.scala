package org.vitrivr.adampro.distribution.partitioning

import org.apache.spark.sql.DataFrame
import org.vitrivr.adampro.data.entity.{Entity, EntityPartitioner}
import org.vitrivr.adampro.data.entity.Entity.AttributeName
import org.vitrivr.adampro.data.index.{Index, IndexPartitioner}
import org.vitrivr.adampro.process.SharedComponentContext

import scala.util.Try

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * August 2017
  */
object PartitioningManager {

  /**
    *
    * @param index
    * @param nPartitions
    * @param joins
    * @param attribute
    * @param mode
    * @param partitioner
    * @param options
    * @return
    */
  def fragment(index: Index, nPartitions: Int, joins: Option[DataFrame], attribute: Option[AttributeName], mode: PartitionMode.Value, partitioner: PartitionerChoice.Value = PartitionerChoice.SPARK, options: Map[String, String] = Map[String, String]())(implicit ac: SharedComponentContext): Try[Index] = {
    IndexPartitioner(index, nPartitions, joins, attribute, mode, partitioner, options)
  }


  /**
    *
    * @param entity
    * @param npartitions
    * @param joins
    * @param attribute
    * @param mode
    * @return
    */
  def fragment(entity : Entity, npartitions: Int, joins: Option[DataFrame], attribute: Option[AttributeName], mode: PartitionMode.Value)(implicit ac: SharedComponentContext): Try[Entity] = {
    EntityPartitioner(entity, npartitions, joins, attribute, mode)
  }

}
