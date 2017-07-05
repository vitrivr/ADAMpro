package org.vitrivr.adampro.query.ast.internal

import org.apache.spark.sql.DataFrame
import org.vitrivr.adampro.process.SharedComponentContext
import org.vitrivr.adampro.shared.cache.QueryCacheOptions
import org.vitrivr.adampro.query.tracker.QueryTracker
import org.vitrivr.adampro.query.ast.generic.{ExpressionDetails, QueryEvaluationOptions, QueryExpression}

/**
  * adamtwo
  *
  * Ivan Giangreco
  * May 2016
  */
case class CacheExpression(private val expr: QueryExpression, private val cache: QueryCacheOptions = QueryCacheOptions(), id: Option[String])(@transient implicit val ac: SharedComponentContext) extends QueryExpression(id) {
  override val info = ExpressionDetails(None, Some("Cache Expression"), id, None)
  _children ++= Seq(expr)

  override protected def run(options : Option[QueryEvaluationOptions], filter: Option[DataFrame] = None)(tracker : QueryTracker)(implicit ac: SharedComponentContext): Option[DataFrame] = {
    log.debug("run cache operation")

    ac.sc.setLocalProperty("spark.scheduler.pool", "slow")
    ac.sc.setJobGroup(id.getOrElse(""), "cache", interruptOnCancel = true)

    if (cache.useCached && id.isDefined) {
      val cached = ac.cacheManager.getQuery(id.get)
      if (cached.isSuccess) {
        return Some(cached.get)
      }
    }

    val res = expr.execute(options)(tracker)
    if (id.isDefined && cache.putInCache) {
      ac.cacheManager.put(id.get, res.get)
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