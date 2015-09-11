package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.cache.RDDCache
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.table.Table
import ch.unibas.dmi.dbis.adam.table.Table._

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
  def apply(tablename : TableName): Unit = {
    val indexes = Index.getIndexnames(tablename)

    indexes.foreach{
      indexname =>
        if(!RDDCache.containsIndex(indexname)) {
          RDDCache.putIndex(Index.getCacheable(indexname))
        }
    }

    if(!RDDCache.containsTable(tablename)) {
      RDDCache.putTable(Table.getCacheable(tablename))
    }
  }
}
