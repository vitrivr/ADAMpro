package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.cache.RDDCache
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity._

/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
object CacheOp {
  /**
   *
   * @param tablename
   */
  def apply(tablename : EntityName): Unit = {
    val indexes = Index.getIndexnames(tablename)

    indexes.foreach{
      indexname =>
        if(!RDDCache.containsIndex(indexname)) {
          RDDCache.putIndex(Index.getCacheable(indexname))
        }
    }

    if(!RDDCache.containsTable(tablename)) {
      RDDCache.putTable(Entity.getCacheable(tablename))
    }
  }
}
