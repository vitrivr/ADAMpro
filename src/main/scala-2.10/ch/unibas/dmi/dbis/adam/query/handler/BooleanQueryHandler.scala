package ch.unibas.dmi.dbis.adam.query.handler

import java.util.concurrent.TimeUnit

import ch.unibas.dmi.dbis.adam.config.AdamConfig
import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.entity.Tuple._
import ch.unibas.dmi.dbis.adam.query.query.BooleanQuery
import ch.unibas.dmi.dbis.adam.query.scanner.MetadataScanner
import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

import scala.util.{Failure, Success, Try}

/**
 * adamtwo
 *
 * Ivan Giangreco
 * November 2015
 */
private[query] object BooleanQueryHandler {
  val log = Logger.getLogger(getClass.getName)
  /**
    * Performs a Boolean query on the metadata.
    *
    * @param entityname
    * @param query
    * @return
    */
  def getIds(entityname: EntityName, query: BooleanQuery): Option[DataFrame] = {
    log.debug("performing metadata-based boolean query on " + entityname)
    BooleanQueryLRUCache.get(entityname, query).get
  }


  /**
    * Returns all metadata tuples from the given entity.
    *
    * @param entityname
    * @return
    */
  def getData(entityname: EntityName, query : Option[BooleanQuery] = None) : Option[DataFrame] = {
    MetadataScanner(Entity.load(entityname).get, query)
  }


  /**
    * Performs a Boolean query on the metadata where the ID only is compared.
    *
    * @param entityname
    * @param filter tuple ids to filter on
    * @return
    */
  def getData(entityname: EntityName, filter: Set[TupleID]): Option[DataFrame] = {
    log.debug("retrieving metadata for " + entityname)
    MetadataScanner(Entity.load(entityname).get, filter)
  }


  object BooleanQueryLRUCache {
    private val maximumCacheSizeIndex = AdamConfig.maximumCacheSizeBooleanQuery
    private val expireAfterAccess = AdamConfig.expireAfterAccessBooleanQuery

    private val bqCache = CacheBuilder.
      newBuilder().
      maximumSize(maximumCacheSizeIndex).
      expireAfterAccess(expireAfterAccess, TimeUnit.MINUTES).
      build(
        new CacheLoader[(EntityName, BooleanQuery), Option[DataFrame]]() {
          def load(query: (EntityName, BooleanQuery)): Option[DataFrame] = {
            val df = MetadataScanner.apply(Entity.load(query._1).get, Option(query._2))

            if(df.isDefined){
              df.get.cache()
            }

            return df
          }
        }
      )


    def get(entityname: EntityName, bq : BooleanQuery): Try[Option[DataFrame]] = {
      try {
        Success(bqCache.get((entityname, bq)))
      } catch {
        case e : Exception =>
          log.error(e.getMessage)
          Failure(e)
      }
    }
  }

}
