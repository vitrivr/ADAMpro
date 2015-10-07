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
case class DefaultTable(tablename : TableName, data : DataFrame) extends Table{
  /**
   *
   * @return
   */
  override def count = data.count()

  /**
   *
   * @return
   */
  override def show() = data.collect()

  /**
   *
   * @param n
   * @return
   */
  override def show(n : Int) = data.take(n)

  /**
   *
   * @param filter
   * @return
   */
  override def rowsForKeys(filter: HashSet[Long]): RDD[Tuple] = {
    tuples.filter(tuple => filter.contains(tuple.tid.toInt))
  }

  /**
   *
   * @return
   */
  override def rows = data.rdd

  /**
   *
   * @return
   */
  override def tuples = data.rdd.map(row => (row : Tuple))
}