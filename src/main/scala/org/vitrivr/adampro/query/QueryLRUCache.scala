package org.vitrivr.adampro.query

import java.util.concurrent.TimeUnit

import org.vitrivr.adampro.config.AdamConfig
import org.vitrivr.adampro.exception.QueryNotCachedException
import org.vitrivr.adampro.utils.Logging
import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.apache.spark.sql.DataFrame
import org.vitrivr.adampro.main.AdamContext

import scala.util.{Failure, Success, Try}

/**
  * adampro
  *
  * Ivan Giangreco
  * April 2016
  */
class QueryLRUCache()(@transient implicit val ac: AdamContext) extends Logging {
  private val maximumCacheSize = ac.config.maximumCacheSizeQueryResults
  private val expireAfterAccess = ac.config.expireAfterAccessQueryResults

  private val queryCache = CacheBuilder.
    newBuilder().
    maximumSize(maximumCacheSize).
    expireAfterAccess(expireAfterAccess, TimeUnit.MINUTES).
    build(
      new CacheLoader[String, DataFrame]() {
        def load(queryid: String): DataFrame = {
          log.trace("cache miss for query " + queryid)
          null
        }
      }
    )

  /**
    * Gets query from cache if it has been performed already.
    *
    * @param queryid
    */
  def get(queryid: String): Try[DataFrame] = {
    try {
      val result = queryCache.getIfPresent(queryid)
      if (result != null) {
        log.debug("getting query results from cache")
        Success(result)
      } else {
        Failure(QueryNotCachedException())
      }
    } catch {
      case e: Exception =>
        Failure(e)
    }
  }

  /**
    * Puts data into query cache
    *
    * @param queryid
    * @param data
    */
  def put(queryid : String, data : DataFrame): Unit ={
    log.debug("putting query results into cache")
    queryCache.put(queryid, data)
  }
}

case class QueryCacheOptions(useCached: Boolean = false, putInCache: Boolean = false)