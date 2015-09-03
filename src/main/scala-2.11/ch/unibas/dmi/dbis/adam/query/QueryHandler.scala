package ch.unibas.dmi.dbis.adam.query

import ch.unibas.dmi.dbis.adam.data.types.Feature._
import ch.unibas.dmi.dbis.adam.index.Index.IndexName
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.query.distance.DistanceFunction
import ch.unibas.dmi.dbis.adam.table.Table.TableName

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
object QueryHandler {
  private val storage = SparkStartup.tableStorage

  def query(q: WorkingVector, distance : DistanceFunction, k : Int, tablename: TableName): Seq[Result] = {
    SequentialScanner(q, distance, k, tablename)
  }

  def indexQuery(q: WorkingVector, distance : DistanceFunction, k : Int, indexname : IndexName, options : Map[String, String]): Seq[Result] = {
    IndexScanner(q, distance, k, indexname, options)
  }
}