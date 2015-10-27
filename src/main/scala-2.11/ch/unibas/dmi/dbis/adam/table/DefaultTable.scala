package ch.unibas.dmi.dbis.adam.table

import ch.unibas.dmi.dbis.adam.table.Table.TableName
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.collection.immutable.HashSet

/**
 * adamtwo
 *
 * Ivan Giangreco
 * October 2015
 */
case class DefaultTable(tablename : TableName, featureData : DataFrame, metadataData : DataFrame) extends Table {
  /**
   *
   * @return
   */
  override def count = featureData.count()

  /**
   *
   * @return
   */
  override def show() = featureData.collect()

  /**
   *
   * @param n
   * @return
   */
  override def show(n : Int) = featureData.take(n)

  /**
   *
   * @param filter
   * @return
   */
  override def tuplesForKeys(filter: HashSet[Long]): RDD[Tuple] = {
    tuples.filter(tuple => filter.contains(tuple.tid.toInt))
  }

  /**
   *
   * @return
   */
  override def rows = featureData.rdd

  /**
   *
   * @return
   */
  override def tuples = featureData.rdd.map(row => (row : Tuple))

  /**
   *
   * @return
   */
  override def getData: DataFrame = featureData

  /**
   *
   * @return
   */
  override def getMetadata : DataFrame = metadataData
}