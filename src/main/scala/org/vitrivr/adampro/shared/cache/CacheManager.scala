package org.vitrivr.adampro.shared.cache

import org.apache.spark.sql.DataFrame
import org.vitrivr.adampro.data.entity.Entity
import org.vitrivr.adampro.data.entity.Entity.EntityName
import org.vitrivr.adampro.data.index.Index
import org.vitrivr.adampro.data.index.Index.IndexName
import org.vitrivr.adampro.process.SharedComponentContext
import org.vitrivr.adampro.shared.catalog.LogManager
import org.vitrivr.adampro.utils.Logging

import scala.util.Try

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * July 2017
  */
class CacheManager()(@transient implicit val ac: SharedComponentContext) extends Serializable with Logging {
  private val entityLRUCache = new LRUCache[EntityName, Entity](ac.config.maximumCacheSizeEntity, ac.config.expireAfterAccessEntity)
  private val indexLRUCache = new LRUCache[IndexName, Index](ac.config.maximumCacheSizeIndex, ac.config.expireAfterAccessIndex)
  private val queryLRUCache = new LRUCache[String, DataFrame](ac.config.maximumCacheSizeQueryResults, ac.config.expireAfterAccessQueryResults)


  // entity
  def containsEntity(entityname: EntityName): Boolean = entityLRUCache.contains(entityname)

  def put(entityname: EntityName, entity: Entity): Unit = entityLRUCache.put(entityname, entity)

  def getEntity(entityname: EntityName): Try[Entity] = entityLRUCache.get(entityname)

  def invalidateEntity(entityname: EntityName): Unit = entityLRUCache.invalidate(entityname)

  def emptyEntity(): Unit = entityLRUCache.empty()


  // index
  def containsIndex(indexname: IndexName): Boolean = indexLRUCache.contains(indexname)

  def put(name: IndexName, index: Index): Unit = indexLRUCache.put(name, index)

  def getIndex(indexname: IndexName): Try[Index] = indexLRUCache.get(indexname)

  def invalidateIndex(indexname: IndexName): Unit = indexLRUCache.invalidate(indexname)

  def emptyIndex(): Unit = indexLRUCache.empty()


  // query
  def getQuery(id: String): Try[DataFrame] = queryLRUCache.get(id)

  def put(id: String, df: DataFrame): Unit = queryLRUCache.put(id, df)
}

object CacheManager {
  /**
    * Create cache manager and fill it
    * @return
    */
  def build()(implicit ac: SharedComponentContext): CacheManager = new CacheManager()(ac)
}