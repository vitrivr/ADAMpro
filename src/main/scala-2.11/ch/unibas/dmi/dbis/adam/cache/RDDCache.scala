package ch.unibas.dmi.dbis.adam.cache

import ch.unibas.dmi.dbis.adam.index.Index.IndexName
import ch.unibas.dmi.dbis.adam.index.{CacheableIndex, Index}
import ch.unibas.dmi.dbis.adam.table.Table.TableName
import ch.unibas.dmi.dbis.adam.table.{CacheableTable, Table}

import scala.collection.mutable.{Map => mMap}

/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
object RDDCache {
  //TODO: Refactor

  val tableCache = mMap[TableName, Table]()
  val indexCache = mMap[IndexName, Index]()

  /**
   *
   * @param cacheableIndex
   */
  def putIndex(cacheableIndex : CacheableIndex): Unit = {
    val indexname = cacheableIndex.index.indexname

    if(!indexCache.contains(indexname)){
      indexCache.put(cacheableIndex.index.indexname, cacheableIndex.index)
    }
  }

  /**
   *
   * @param indexname
   * @return
   */
  def containsIndex(indexname : IndexName): Boolean ={
    indexCache.contains(indexname)
  }

  /**
   *
   * @param indexname
   * @return
   */
  def getIndex(indexname : IndexName) : Index = {
    indexCache.getOrElse(indexname, null)
  }



  /**
   *
   * @param cacheableTable
   */
  def putTable(cacheableTable : CacheableTable): Unit = {
    val tablename = cacheableTable.table.tablename

    if(!tableCache.contains(tablename)){
      tableCache.put(cacheableTable.table.tablename, cacheableTable.table)
    }
  }


  /**
   *
   * @param tableName
   * @return
   */
  def containsTable(tableName: TableName): Boolean ={
    tableCache.contains(tableName)
  }

  /**
   *
   * @param tableName
   * @return
   */
  def getTable(tableName: TableName) : Table = {
    tableCache.getOrElse(tableName, null)
  }

}
