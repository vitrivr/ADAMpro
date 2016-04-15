package ch.unibas.dmi.dbis.adam.index

import java.util.concurrent.TimeUnit

import ch.unibas.dmi.dbis.adam.config.AdamConfig
import ch.unibas.dmi.dbis.adam.index.Index.IndexName
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
object IndexLRUCache {
  val log = Logger.getLogger(getClass.getName)

  private val maximumCacheSize = AdamConfig.maximumCacheSizeIndex
  private val expireAfterAccess = AdamConfig.expireAfterAccessIndex

  private val indexCache = CacheBuilder.
    newBuilder().
    maximumSize(maximumCacheSize).
    expireAfterAccess(expireAfterAccess, TimeUnit.MINUTES).
    build(
      new CacheLoader[IndexName, Index]() {
        def load(indexname: IndexName): Index = {
          import SparkStartup.Implicits._
          val index = IndexHandler.loadIndexMetaData(indexname).get
          index
        }
      }
    )

  /**
    * Gets index from cache. If index is not yet in cache, it is loaded.
    *
    * @param indexname
    */
  def get(indexname: IndexName): Try[Index] = {
    try {
      Success(indexCache.get(indexname))
    } catch {
      case e: Exception =>
        log.error(e.getMessage)
        Failure(e)
    }
  }
}

