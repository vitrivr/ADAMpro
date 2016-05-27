package ch.unibas.dmi.dbis.adam.query.handler.internal

import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.datastructures.{QueryLRUCache, QueryCacheOptions}
import ch.unibas.dmi.dbis.adam.query.handler.generic.{ExpressionDetails, QueryExpression}
import org.apache.spark.sql.DataFrame

/**
  * adamtwo
  *
  * Ivan Giangreco
  * May 2016
  */
case class CacheExpression(expr: QueryExpression, cache: QueryCacheOptions = QueryCacheOptions(), id: Option[String])(implicit ac: AdamContext) extends QueryExpression(id) {
  override val info = ExpressionDetails(None, Some("Cache Expression"), id, None)
  children ++= Seq(expr)

  override protected def run(filter: Option[DataFrame] = None)(implicit ac: AdamContext): Option[DataFrame] = {
    if (cache.useCached && id.isDefined) {
      val cached = QueryLRUCache.get(id.get)
      if (cached.isSuccess) {
        return Some(cached.get)
      }
    }

    val res = expr.evaluate()
    if (id.isDefined && cache.putInCache) {
      QueryLRUCache.put(id.get, res.get)
    }
    return res
  }
}