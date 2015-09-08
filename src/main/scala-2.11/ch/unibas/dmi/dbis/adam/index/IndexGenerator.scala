package ch.unibas.dmi.dbis.adam.index

import ch.unibas.dmi.dbis.adam.data.types.Feature.{WorkingVector, VectorBase}
import ch.unibas.dmi.dbis.adam.data.types.Feature._
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.table.Table._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

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
  def index(indexname : IndexName, tablename : TableName, data: DataFrame): Index = {
    val rdd = data.map { x => IndexerTuple(x.getInt(0), x.getSeq[VectorBase](1) : WorkingVector) }
    index(indexname, tablename, rdd)
  }

  /**
   *
   */
  def index(indexname : IndexName, tablename : TableName, data: RDD[IndexerTuple[WorkingVector]]): Index
}
