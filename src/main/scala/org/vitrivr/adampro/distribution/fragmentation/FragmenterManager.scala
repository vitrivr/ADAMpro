package org.vitrivr.adampro.distribution.fragmentation

import org.apache.spark.sql.DataFrame
import org.vitrivr.adampro.data.entity.{Entity, EntityFragmenter}
import org.vitrivr.adampro.data.entity.Entity.AttributeName
import org.vitrivr.adampro.data.index.{Index, IndexFragmenter}
import org.vitrivr.adampro.process.SharedComponentContext

import scala.util.Try

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * August 2017
  */
object FragmenterManager {

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
    IndexFragmenter(index, nPartitions, joins, attribute, mode, partitioner, options)
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
    EntityFragmenter(entity, npartitions, joins, attribute, mode)
  }

}
