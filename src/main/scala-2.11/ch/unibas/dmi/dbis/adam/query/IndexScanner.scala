package ch.unibas.dmi.dbis.adam.query

import ch.unibas.dmi.dbis.adam.datatypes.Feature._
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.index.Index.IndexName
import ch.unibas.dmi.dbis.adam.query.distance.DistanceFunction

import scala.collection.immutable.BitSet


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
   * @param indexname
   * @param options
   * @return
   */
  def apply(q: WorkingVector, distance : DistanceFunction, k : Int, indexname: IndexName, options : Map[String, String]): BitSet = {
    Index.retrieveIndex(indexname).scan(q, options)
  }
}
