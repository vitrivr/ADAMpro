package ch.unibas.dmi.dbis.adam.query.handler.internal

import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.datastructures.{QueryCacheOptions, QueryExpression, QueryLRUCache}
import ch.unibas.dmi.dbis.adam.query.handler.QueryHints.{ComplexQueryHint, IndexQueryHint, QueryHint, SEQUENTIAL_QUERY}
import ch.unibas.dmi.dbis.adam.query.handler.{BooleanQueryHandler, QueryHints}
import ch.unibas.dmi.dbis.adam.query.query.{BooleanQuery, NearestNeighbourQuery, PrimaryKeyFilter}
import ch.unibas.dmi.dbis.adam.storage.engine.CatalogOperator
import org.apache.spark.sql.DataFrame

/**
  * adamtwo
  *
  * Ivan Giangreco
  * May 2016
  */
case class StandardQueryHolder(entityname: EntityName)(hint: Seq[QueryHint], nnq: Option[NearestNeighbourQuery], bq: Option[BooleanQuery], tiq: Option[PrimaryKeyFilter[_]], id: Option[String] = None, cache: Option[QueryCacheOptions] = Some(QueryCacheOptions())) extends QueryExpression(id) {
  override protected def run(filter: Option[DataFrame])(implicit ac: AdamContext): DataFrame = {
    val annq = if (nnq.isDefined) {
      Option(NearestNeighbourQuery(nnq.get.column, nnq.get.q, nnq.get.distance, nnq.get.k, nnq.get.indexOnly, nnq.get.options, nnq.get.partitions, nnq.get.queryID))
    } else {
      None
    }
    val atiq = if (tiq.isDefined) {
      Some(tiq.get.+:(filter))
    } else {
      if (filter.isDefined) {
        Some(new PrimaryKeyFilter(filter.get))
      } else {
        None
      }
    }
    query(entityname, hint, annq, bq, atiq, id, cache)
  }


  /**
    * Performs a standard query, built up by a nearest neighbour query and a boolean query.
    *
    * @param entityname
    * @param hint         query hint, for the executor to know which query path to take (e.g., sequential query or index query)
    * @param nnq          information for nearest neighbour query
    * @param bq           information for boolean query
    * @param id           query id
    * @param cache        caching options
    * @return
    */
  def query(entityname: EntityName, hint: Seq[QueryHint], nnq: Option[NearestNeighbourQuery], bq: Option[BooleanQuery], tiq: Option[PrimaryKeyFilter[_]], id: Option[String] = None, cache: Option[QueryCacheOptions] = Some(QueryCacheOptions()))(implicit ac: AdamContext): DataFrame = {
    if (cache.isDefined && cache.get.useCached && id.isDefined) {
      val cached = QueryLRUCache.get(id.get)
      if (cached.isSuccess) {
        return cached.get
      }
    }

    val entity = Entity.load(entityname).get

    if (nnq.isEmpty) {
      if (bq.isDefined) {
        BooleanQueryHandler.filter(entity.data, bq.get)
      } else {
        entity.data
      }
    }

    val indexes: Map[IndexTypeName, Seq[IndexName]] = CatalogOperator.listIndexes(entityname).groupBy(_._2).mapValues(_.map(_._1))

    log.debug("try choosing plan based on hints")
    var plan = choosePlan(entityname, indexes, hint, nnq)

    if (!plan.isDefined) {
      log.debug("no query plan chosen, go to fallback")
      plan = choosePlan(entityname, indexes, Seq(QueryHints.FALLBACK_HINTS), nnq)
    }

    val res = plan.get(nnq.get, bq, tiq, id, cache).evaluate()

    if (id.isDefined && cache.isDefined && cache.get.putInCache) {
      QueryLRUCache.put(id.get, res)
    }

    res
  }

  /**
    * Chooses the query plan to use based on the given hint, the available indexes, etc.
    *
    * @param entityname
    * @param indexes
    * @param hints
    * @return
    */
  private def choosePlan(entityname: EntityName, indexes: Map[IndexTypeName, Seq[IndexName]], hints: Seq[QueryHint], nnq: Option[NearestNeighbourQuery])(implicit ac: AdamContext) : Option[(NearestNeighbourQuery, Option[BooleanQuery], Option[PrimaryKeyFilter[_]], Option[String], Option[QueryCacheOptions]) => QueryExpression] = {


    if (hints.isEmpty) {
      log.debug("no execution plan hint")
      return None
    }

    if (!nnq.isDefined) {
      log.debug("nnq is not defined")
      return None
    }

    var j = 0
    while (j < hints.length) {
      hints(j) match
      {
        case iqh: IndexQueryHint => {
          log.debug("index execution plan hint")
          //index scan
          val indexChoice = indexes.get(iqh.structureType)

          if (indexChoice.isDefined) {
            val indexes = indexChoice.get
              .map(indexname => Index.load(indexname, false).get)
              .filter(_.isQueryConform(nnq.get)) //choose only indexes that are conform to query
              .filterNot(_.isStale) //don't use stale indexes
              .sortBy(-_.weight) //order by weight (highest weight first)

            if (indexes.isEmpty) {
              return None
            }

            return Some(IndexQueryHolder(indexes.head))
          } else {
            return None
          }
        }
        case SEQUENTIAL_QUERY =>
          log.debug("sequential execution plan hint")
          return Some(SequentialQueryHolder(entityname)) //sequential

        case cqh: ComplexQueryHint => {
          log.debug("compound query hint, re-iterate sub-hints")

          //complex query hint
          val chint = cqh.hints
          var i = 0

          while (i < chint.length) {
            val plan = choosePlan(entityname, indexes, Seq(chint(i)), nnq)
            if (plan.isDefined) return plan

            i += 1
          }

          return None
        }
        case _ => None //default
      }

      j += 1
    }

    None
  }
}