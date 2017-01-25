package org.vitrivr.adampro.index

import java.util.concurrent.TimeUnit

import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.vitrivr.adampro.catalog.CatalogOperator
import org.vitrivr.adampro.config.AdamConfig
import org.vitrivr.adampro.exception.IndexNotExistingException
import org.vitrivr.adampro.index.Index.IndexName
import org.vitrivr.adampro.main.{AdamContext, SparkStartup}
import org.vitrivr.adampro.utils.Logging

import scala.util.{Failure, Success, Try}

/**
  * adampro
  *
  * Ivan Giangreco
  * April 2016
  */
class IndexLRUCache()(@transient implicit val ac: AdamContext) extends Logging {

  private val maximumCacheSize = ac.config.maximumCacheSizeIndex
  private val expireAfterAccess = ac.config.expireAfterAccessIndex

  private val indexCache = CacheBuilder.
    newBuilder().
    maximumSize(maximumCacheSize).
    expireAfterAccess(expireAfterAccess, TimeUnit.MINUTES).
    build(
      new CacheLoader[IndexName, Index]() {
        def load(indexname: IndexName): Index = {
          log.trace("cache miss for index " + indexname + "; loading from disk")
          val index = Index.loadIndexMetaData(indexname)(ac).get
          index
        }
      }
    )

  /**
    * Gets index from cache. If index is not yet in cache, it is loaded.
    *
    * @param indexname name of index
    */
  def get(indexname: IndexName): Try[Index] = {
    try {
      log.debug("getting index " + indexname + " from cache")
      if(indexCache.asMap().containsKey(indexname) || SparkStartup.catalogOperator.existsIndex(indexname).get){
        Success(indexCache.get(indexname))
      } else {
        throw new IndexNotExistingException()
      }
    } catch {
      case e: Exception =>
        log.error("index " + indexname + " could not be found in cache and could not be loaded")
        Failure(e)
    }
  }

  /**
    *
    * @param indexname name of index
    * @return
    */
  def contains(indexname : IndexName) : Boolean = {
    indexCache.getIfPresent(indexname) != null
  }

  /**
    *
    * @param indexname name of index
    * @param index index
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
    * @param indexname name of index
    */
  def invalidate(indexname : IndexName): Unit = {
    indexCache.invalidate(indexname)
  }
}

