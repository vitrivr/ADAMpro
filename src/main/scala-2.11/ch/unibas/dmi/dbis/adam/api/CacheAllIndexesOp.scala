package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.cache.RDDCache
import ch.unibas.dmi.dbis.adam.index.Index

/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
object CacheAllIndexesOp {
  /**
   *
   */
  def apply(): Unit = {
    val indexes = Index.getIndexnames()

    indexes.foreach{
      indexname =>
        if(!RDDCache.containsIndex(indexname)) {
          RDDCache.putIndex(Index.getCacheable(indexname))
        }
    }
  }
}
