package ch.unibas.dmi.dbis.adam.index

import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.index.Index.IndexName
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import org.apache.spark.rdd.RDD

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
trait IndexGenerator {
  def indextypename: IndexTypes.IndexType
  def index(indexname : IndexName, entityname : EntityName, data: RDD[IndexingTaskTuple[_]]):  Index
}
