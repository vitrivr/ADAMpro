package ch.unibas.dmi.dbis.adam.query.handler.internal

import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.datastructures.{QueryCacheOptions, QueryExpression, QueryLRUCache}
import ch.unibas.dmi.dbis.adam.query.handler.{BooleanQueryHandler, NearestNeighbourQueryHandler}
import ch.unibas.dmi.dbis.adam.query.query.{BooleanQuery, NearestNeighbourQuery, PrimaryKeyFilter}
import org.apache.spark.sql.DataFrame

/**
  * adamtwo
  *
  * Ivan Giangreco
  * May 2016
  */
case class IndexQueryHolder(index: Index)(nnq: NearestNeighbourQuery, bq: Option[BooleanQuery], tiq: Option[PrimaryKeyFilter[_]], withMetadata: Boolean, id: Option[String] = None, cache: Option[QueryCacheOptions] = Some(QueryCacheOptions())) extends QueryExpression(id) {
  def this(indexname: IndexName)(nnq: NearestNeighbourQuery, bq: Option[BooleanQuery], tiq: Option[PrimaryKeyFilter[_]], withMetadata: Boolean, id: Option[String], cache: Option[QueryCacheOptions])(implicit ac: AdamContext) {
    this(Index.load(indexname).get)(nnq, bq, tiq, withMetadata, id, cache)
  }

  def this(entityname: EntityName, indextypename: IndexTypeName)(nnq: NearestNeighbourQuery, bq: Option[BooleanQuery], tiq: Option[PrimaryKeyFilter[_]], withMetadata: Boolean, id: Option[String] = None, cache: Option[QueryCacheOptions] = Some(QueryCacheOptions()))(implicit ac: AdamContext) {
    //TODO: throw error if not head possible
    this(Entity.load(entityname).get.indexes
        .filter(_.isSuccess).map(_.get)
        .filter(_.isQueryConform(nnq)) //choose only indexes that are conform to query
        .sortBy(-_.weight).head)(nnq, bq, tiq, withMetadata, id, cache)
  }

  override protected def run(filter: Option[DataFrame])(implicit ac: AdamContext): DataFrame = {
    val annq = NearestNeighbourQuery(nnq.column, nnq.q, nnq.distance, nnq.k, nnq.indexOnly, nnq.options, nnq.partitions, nnq.queryID)
    val atiq = if (tiq.isDefined) {
      Some(tiq.get.+:(filter))
    } else {
      if (filter.isDefined) {
        Some(new PrimaryKeyFilter(filter.get))
      } else {
        None
      }
    }
    query(index)(annq, bq, atiq, false, id, cache)
  }

  /**
    * Performs a index-based query.
    *
    * @param index
    * @param nnq
    * @param bq
    * @param withMetadata
    * @param id
    * @param cache
    * @return
    */
  def query(index: Index)(nnq: NearestNeighbourQuery, bq: Option[BooleanQuery], tiq: Option[PrimaryKeyFilter[_]], withMetadata: Boolean, id: Option[String] = None, cache: Option[QueryCacheOptions] = Some(QueryCacheOptions()))(implicit ac: AdamContext): DataFrame = {
    if (cache.isDefined && cache.get.useCached && id.isDefined) {
      val cached = QueryLRUCache.get(id.get)
      if (cached.isSuccess) {
        return cached.get
      }
    }

    val entityname = index.entityname

    log.debug("index query gets filter")
    val filter = BooleanQueryHandler.getFilter(entityname, bq, tiq)

    if (!index.isQueryConform(nnq)) {
      log.warn("index is not conform with kNN query")
    }

    log.debug("index query performs kNN query")
    var res = NearestNeighbourQueryHandler.indexQuery(index, nnq, filter)

    if (withMetadata) {
      log.debug("join metadata to results of index query")
      val entity = Entity.load(entityname).get
      var data = entity.data
      var pk = entity.pk

      res = res.join(data, pk.name)
    }

    if (id.isDefined && cache.isDefined && cache.get.putInCache) {
      QueryLRUCache.put(id.get, res)
    }

    res
  }
}