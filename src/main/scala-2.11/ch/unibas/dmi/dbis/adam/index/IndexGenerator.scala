package ch.unibas.dmi.dbis.adam.index

import ch.unibas.dmi.dbis.adam.datatypes.Feature.WorkingVector
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.table.Table._
import org.apache.spark.rdd.RDD

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
trait IndexGenerator {
  def indextypename: IndexTypeName

  /**
   *
   */
  def index(indexname : IndexName, tablename : TableName, data: RDD[IndexerTuple[WorkingVector]]):  Index[_ <: IndexTuple]
}
