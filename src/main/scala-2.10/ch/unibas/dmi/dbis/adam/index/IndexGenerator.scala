package ch.unibas.dmi.dbis.adam.index

import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.entity.Entity._
import org.apache.spark.rdd.RDD

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
trait IndexGenerator {
  def indextypename: IndexTypeName
  def index(indexname : IndexName, entityname : EntityName, data: RDD[IndexingTaskTuple]):  Index[_ <: IndexTuple]
}
