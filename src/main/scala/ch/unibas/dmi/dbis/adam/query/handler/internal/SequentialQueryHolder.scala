package ch.unibas.dmi.dbis.adam.query.handler.internal

import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
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
case class SequentialQueryHolder(entityname: EntityName)(nnq: NearestNeighbourQuery, bq: Option[BooleanQuery], tiq: Option[PrimaryKeyFilter[_]], id: Option[String] = None, cache: Option[QueryCacheOptions] = Some(QueryCacheOptions())) extends QueryExpression(id) {
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
    query(entityname)(annq, bq, atiq, id, cache)
  }

  /**
    * Performs a sequential query, i.e., without using any index structure.
    *
    * @param entityname
    * @param nnq          information for nearest neighbour query
    * @param bq           information for boolean query
    * @return
    */
  def query(entityname: EntityName)(nnq: NearestNeighbourQuery, bq: Option[BooleanQuery], tiq: Option[PrimaryKeyFilter[_]], id: Option[String] = None, cache: Option[QueryCacheOptions] = Some(QueryCacheOptions()))(implicit ac: AdamContext): DataFrame = {
    if (cache.isDefined && cache.get.useCached && id.isDefined) {
      val cached = QueryLRUCache.get(id.get)
      if (cached.isSuccess) {
        return cached.get
      }
    }

    val filter = BooleanQueryHandler.getFilter(entityname, bq, tiq)

    log.debug("sequential query performs kNN query")
    var res = NearestNeighbourQueryHandler.sequential(entityname, nnq, filter)

    if (id.isDefined && cache.isDefined && cache.get.putInCache) {
      QueryLRUCache.put(id.get, res)
    }

    res
  }
}



