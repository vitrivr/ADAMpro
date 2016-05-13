package ch.unibas.dmi.dbis.adam.query.handler.internal

import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.datastructures.{QueryLRUCache, QueryCacheOptions, QueryExpression}
import ch.unibas.dmi.dbis.adam.query.handler.{BooleanQueryHandler, NearestNeighbourQueryHandler}
import ch.unibas.dmi.dbis.adam.query.query.{BooleanQuery, NearestNeighbourQuery, PrimaryKeyFilter}
import org.apache.spark.sql.DataFrame

/**
  * adamtwo
  *
  * Ivan Giangreco
  * May 2016
  */
case class SequentialQueryHolder(entityname: EntityName)(nnq: NearestNeighbourQuery, bq: Option[BooleanQuery], tiq: Option[PrimaryKeyFilter[_]], withMetadata : Boolean, id: Option[String] = None, cache: Option[QueryCacheOptions] = Some(QueryCacheOptions())) extends QueryExpression(id) {
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
    query(entityname)(annq, bq, atiq, withMetadata, id, cache)
  }

  /**
    * Performs a sequential query, i.e., without using any index structure.
    *
    * @param entityname
    * @param nnq          information for nearest neighbour query
    * @param bq           information for boolean query
    * @param withMetadata whether or not to retrieve corresponding metadata
    * @return
    */
  def query(entityname: EntityName)(nnq: NearestNeighbourQuery, bq: Option[BooleanQuery], tiq: Option[PrimaryKeyFilter[_]], withMetadata: Boolean, id: Option[String] = None, cache: Option[QueryCacheOptions] = Some(QueryCacheOptions()))(implicit ac: AdamContext): DataFrame = {
    if (cache.isDefined && cache.get.useCached && id.isDefined) {
      val cached = QueryLRUCache.get(id.get)
      if (cached.isSuccess) {
        return cached.get
      }
    }

    val filter = BooleanQueryHandler.getFilter(entityname, bq, tiq)

    log.debug("sequential query performs kNN query")
    var res = NearestNeighbourQueryHandler.sequential(entityname, nnq, filter)

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



