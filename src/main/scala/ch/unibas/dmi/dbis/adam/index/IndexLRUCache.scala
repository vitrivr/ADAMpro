package ch.unibas.dmi.dbis.adam.index

import java.util.concurrent.TimeUnit

import ch.unibas.dmi.dbis.adam.config.AdamConfig
import ch.unibas.dmi.dbis.adam.index.Index.IndexName
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.apache.spark.Logging

import scala.util.{Failure, Success, Try}

/**
  * adampro
  *
  * Ivan Giangreco
  * April 2016
  */
object IndexLRUCache extends Logging {

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
          log.trace("cache miss for index " + indexname + "; loading from disk")
          val index = Index.loadIndexMetaData(indexname).get
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
      log.trace("getting index " + indexname + " from cache")
      Success(indexCache.get(indexname))
    } catch {
      case e: Exception =>
        Failure(e)
    }
  }

  /**
    *
    * @param indexname
    * @return
    */
  def contains(indexname : IndexName) : Boolean = {
    indexCache.getIfPresent(indexname) != null
  }

  /**
    *
    * @param indexname
    * @param index
    * @return
    */
  def put(indexname : IndexName, index : Index) : Unit = {
    log.debug("putting index " + indexname + " manually into cache")
    indexCache.put(indexname, index)
  }

  /**
    *
    */
  def empty() : Unit = {
    indexCache.invalidateAll()
  }

  /**
    *
    * @param indexname
    */
  def invalidate(indexname : IndexName): Unit = {
    indexCache.invalidate(indexname)
  }
}

