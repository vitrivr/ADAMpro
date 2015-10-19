package ch.unibas.dmi.dbis.adam.table

import ch.unibas.dmi.dbis.adam.storage.components.LazyTableStorage
import ch.unibas.dmi.dbis.adam.table.Table.TableName
import ch.unibas.dmi.dbis.adam.table.Tuple._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.immutable.HashSet

/**
 * adamtwo
 *
 * Ivan Giangreco
 * October 2015
 */
case class LazyTable(tablename : TableName, storage : LazyTableStorage)  extends Table{
  /**
   *
   * @return
   */
  override def count: TupleID = rows.count()

  /**
   *
   * @return
   */
  override def tuples: RDD[Tuple] = rows.map(r => (r : Tuple))

  /**
   *
   * @return
   */
  override def rows: RDD[Row] = storage.readFullTable(tablename).rdd

  /**
   *
   * @param filter
   * @return
   */
  override def tuplesForKeys(filter: HashSet[TupleID]): RDD[Tuple] = storage.readFilteredTable(tablename, filter).map(r => (r : Tuple))

  /**
   *
   * @return
   */
  override def show(): Array[Row] = ???

  /**
   *
   * @param n
   * @return
   */
  override def show(n: Int): Array[Row] = ???

  /**
   *
   * @return
   */
  override def getData: DataFrame = ???
}
