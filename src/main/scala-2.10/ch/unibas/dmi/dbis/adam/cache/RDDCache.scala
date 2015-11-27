package ch.unibas.dmi.dbis.adam.cache

import ch.unibas.dmi.dbis.adam.index.Index.IndexName
import ch.unibas.dmi.dbis.adam.index.{CacheableIndex, Index, IndexTuple}
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.entity.{CacheableEntity, Entity}

import scala.collection.mutable.{Map => mMap}

/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
 object RDDCache {

  val tableCache = mMap[EntityName, Entity]()
  val indexCache = mMap[IndexName,  Index[_ <: IndexTuple]]()

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
  def getIndex(indexname : IndexName) :  Index[_ <: IndexTuple] = {
    indexCache.getOrElse(indexname, null)
  }



  /**
   *
   * @param cacheableTable
   */
  def putTable(cacheableTable : CacheableEntity): Unit = {
    val tablename = cacheableTable.entity.entityname

    if(!tableCache.contains(tablename)){
      tableCache.put(cacheableTable.entity.entityname, cacheableTable.entity)
    }
  }


  /**
   *
   * @param tableName
   * @return
   */
  def containsTable(tableName: EntityName): Boolean ={
    tableCache.contains(tableName)
  }

  /**
   *
   * @param tableName
   * @return
   */
  def getTable(tableName: EntityName) : Entity = {
    tableCache.getOrElse(tableName, null)
  }

}
