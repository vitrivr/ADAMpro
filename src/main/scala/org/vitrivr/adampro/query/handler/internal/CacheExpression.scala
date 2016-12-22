package org.vitrivr.adampro.query.handler.internal

import org.apache.spark.sql.DataFrame
import org.vitrivr.adampro.main.AdamContext
import org.vitrivr.adampro.query.QueryCacheOptions
import org.vitrivr.adampro.query.handler.generic.{ExpressionDetails, QueryEvaluationOptions, QueryExpression}

/**
  * adamtwo
  *
  * Ivan Giangreco
  * May 2016
  */
case class CacheExpression(private val expr: QueryExpression, private val cache: QueryCacheOptions = QueryCacheOptions(), id: Option[String])(@transient implicit val ac: AdamContext) extends QueryExpression(id) {
  override val info = ExpressionDetails(None, Some("Cache Expression"), id, None)
  _children ++= Seq(expr)

  override protected def run(options : Option[QueryEvaluationOptions], filter: Option[DataFrame] = None)(implicit ac: AdamContext): Option[DataFrame] = {
    log.debug("run cache operation")

    ac.sc.setLocalProperty("spark.scheduler.pool", "slow")
    ac.sc.setJobGroup(id.getOrElse(""), "cache", interruptOnCancel = true)

    if (cache.useCached && id.isDefined) {
      val cached = ac.queryLRUCache.get(id.get)
      if (cached.isSuccess) {
        return Some(cached.get)
      }
    }

    val res = expr.evaluate(options)
    if (id.isDefined && cache.putInCache) {
      ac.queryLRUCache.put(id.get, res.get)
    }
    return res
  }

  override def equals(that: Any): Boolean =
    that match {
      case that: CacheExpression => this.expr.equals(that.expr)
      case _ => expr.equals(that)
    }

  override def hashCode(): Int = expr.hashCode
}