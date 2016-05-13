package ch.unibas.dmi.dbis.adam.entity

import java.util.concurrent.TimeUnit

import ch.unibas.dmi.dbis.adam.config.AdamConfig
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.apache.log4j.Logger

import scala.util.{Failure, Success, Try}

/**
  * adampro
  *
  * Ivan Giangreco
  * April 2016
  */
object EntityLRUCache {
  val log = Logger.getLogger(getClass.getName)

  private val maximumCacheSize = AdamConfig.maximumCacheSizeEntity
  private val expireAfterAccess = AdamConfig.expireAfterAccessEntity

  private val entityCache = CacheBuilder.
    newBuilder().
    maximumSize(maximumCacheSize).
    expireAfterAccess(expireAfterAccess, TimeUnit.MINUTES).
    build(
      new CacheLoader[EntityName, Entity]() {
        def load(entityname: EntityName): Entity = {
          import SparkStartup.Implicits._
          val entity = Entity.loadEntityMetaData(entityname)
          entity.get
        }
      }
    )

  /**
    * Gets entity from cache. If entity is not yet in cache, it is loaded.
    *
    * @param entityname
    */
  def get(entityname: EntityName): Try[Entity] = {
    try {
      Success(entityCache.get(entityname))
    } catch {
      case e: Exception =>
        log.error(e.getMessage)
        Failure(e)
    }
  }
}

