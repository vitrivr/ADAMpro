package ch.unibas.dmi.dbis.adam.query.datastructures

import java.util.concurrent.TimeUnit

import ch.unibas.dmi.dbis.adam.config.AdamConfig
import ch.unibas.dmi.dbis.adam.exception.QueryNotCachedException
import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

import scala.util.{Failure, Success, Try}

/**
  * adampro
  *
  * Ivan Giangreco
  * April 2016
  */
private[query] object QueryLRUCache {
  val log = Logger.getLogger(getClass.getName)

  private val maximumCacheSize = AdamConfig.maximumCacheSizeQueryResults
  private val expireAfterAccess = AdamConfig.expireAfterAccessQueryResults

  private val queryCache = CacheBuilder.
    newBuilder().
    maximumSize(maximumCacheSize).
    expireAfterAccess(expireAfterAccess, TimeUnit.MINUTES).
    build(
      new CacheLoader[String, DataFrame]() {
        def load(queryid: String): DataFrame = {
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
        Success(result)
      } else {
        Failure(QueryNotCachedException())
      }
    } catch {
      case e: Exception =>
        log.error(e.getMessage)
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
    queryCache.put(queryid, data)
  }
}

case class QueryCacheOptions(useCached: Boolean = false, putInCache: Boolean = false)

