package org.vitrivr.adampro.entity

import java.util.concurrent.TimeUnit

import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.vitrivr.adampro.entity.Entity.EntityName
import org.vitrivr.adampro.exception.EntityNotExistingException
import org.vitrivr.adampro.main.{AdamContext, SparkStartup}
import org.vitrivr.adampro.utils.Logging

import scala.util.{Failure, Success, Try}

/**
  * adampro
  *
  * Ivan Giangreco
  * April 2016
  */
class EntityLRUCache()(@transient implicit val ac: AdamContext) extends Logging {

  private val maximumCacheSize = ac.config.maximumCacheSizeEntity
  private val expireAfterAccess = ac.config.expireAfterAccessEntity

  private val entityCache = CacheBuilder.
    newBuilder().
    maximumSize(maximumCacheSize).
    expireAfterAccess(expireAfterAccess, TimeUnit.MINUTES).
    build(
      new CacheLoader[EntityName, Entity]() {
        def load(entityname: EntityName): Entity = {
          log.trace("cache miss for entity " + entityname + "; loading and caching")
          val entity = Entity.loadEntityMetaData(entityname)(ac)
          entity.get
        }
      }
    )

  /**
    * Gets entity from cache. If entity is not yet in cache, it is loaded.
    *
    * @param entityname name of entity
    */
  def get(entityname: EntityName): Try[Entity] = {
    try {
      log.trace("getting entity " + entityname + " from cache")
      if(entityCache.asMap().containsKey(entityname) || SparkStartup.catalogOperator.existsEntity(entityname).get){
        Success(entityCache.get(entityname))
      } else {
        throw new EntityNotExistingException()
      }
    } catch {
      case e: Exception =>
        log.error("entity " + entityname + " could not be found in cache and could not be loaded")
        Failure(e)
    }
  }

  /**
    * Invalidates the cache entry for entity.
    *
    * @param entityname name of entity
    */
  def invalidate(entityname: EntityName): Unit = {
    entityCache.invalidate(entityname)
  }
}

