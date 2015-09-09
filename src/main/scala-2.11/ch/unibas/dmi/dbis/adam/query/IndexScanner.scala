package ch.unibas.dmi.dbis.adam.query

import ch.unibas.dmi.dbis.adam.datatypes.Feature
import ch.unibas.dmi.dbis.adam.table.Tuple
import Tuple.TupleID
import Feature._
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.index.Index.IndexName
import ch.unibas.dmi.dbis.adam.query.distance.DistanceFunction
import org.apache.spark.FutureAction


/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
object IndexScanner {
  /**
   *
   * @param q
   * @param distance
   * @param k
   * @param indexName
   * @param options
   * @return
   */
  def apply(q: WorkingVector, distance : DistanceFunction, k : Int, indexName: IndexName, options : Map[String, String]): FutureAction[Seq[TupleID]] = {
    Index.retrieveIndex(indexName).scan(q, options)
  }
}
