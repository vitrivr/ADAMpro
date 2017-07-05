package org.vitrivr.adampro.shared.cache

import java.util.concurrent.TimeUnit

import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.vitrivr.adampro.utils.exception.ObjectNotInCacheException
import org.vitrivr.adampro.process.SharedComponentContext
import org.vitrivr.adampro.utils.Logging

import scala.util.{Failure, Success, Try}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * July 2017
  */
class LRUCache[N, O](maximumCacheSize : Int, expireAfterAccess : Int) extends Logging {

  private val cache  = CacheBuilder.
    newBuilder().
    maximumSize(maximumCacheSize).
    expireAfterAccess(expireAfterAccess, TimeUnit.MINUTES).
    build(
      new CacheLoader[String, Object]() {
        def load(name : String): Object = {
          log.trace("cache miss for entity " + name + "; loading and caching")
          null
        }
      }
    )

  /**
    *
    * @param name name of object
    * @param obj object
    * @return
    */
  def put(name : N, obj : O) : Unit = {
    log.debug("putting object " + name + " manually into cache")
    cache.put(name.toString, obj.asInstanceOf[Object])
  }

  /**
    *
    * @param name name of object
    * @return
    */
  def contains(name : N) : Boolean = {
    cache.asMap().containsKey(name)
  }

  /**
    * Gets object from cache. If object is not yet in cache, Failure is returned.
    *
    * @param name name of object
    */
  def get(name: N): Try[O] = {
    try {
      log.trace("getting object " + name.toString + " from cache")
      if(cache.asMap().containsKey(name)){
        Success(cache.get(name.toString).asInstanceOf[O])
      } else {
        throw new ObjectNotInCacheException()
      }
    } catch {
      case e: Exception =>
        Failure(e)
    }
  }

  /**
    * Invalidates the cache entry.
    *
    * @param name name of object
    */
  def invalidate(name : N): Unit = {
    cache.invalidate(name.toString)
  }


  /**
    *
    */
  def empty() : Unit = {
    cache.invalidateAll()
  }
}