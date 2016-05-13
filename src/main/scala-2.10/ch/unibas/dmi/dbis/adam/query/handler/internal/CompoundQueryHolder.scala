package ch.unibas.dmi.dbis.adam.query.handler.internal

import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.Result
import ch.unibas.dmi.dbis.adam.query.datastructures.{QueryLRUCache, RunDetails, QueryCacheOptions, QueryExpression}
import ch.unibas.dmi.dbis.adam.query.handler.FeatureScanner
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery
import org.apache.spark.sql.{Row, DataFrame}

import scala.collection.mutable.ListBuffer

/**
  * adamtwo
  *
  * Ivan Giangreco
  * May 2016
  */
case class CompoundQueryHolder(entityname: EntityName)(expr: QueryExpression, nnq: Option[NearestNeighbourQuery], withMetadata: Boolean = true, id: Option[String] = None, cache: Option[QueryCacheOptions] = Some(QueryCacheOptions())) extends QueryExpression(id) {
  private var run = false

  override protected def run(filter: Option[DataFrame])(implicit ac: AdamContext): DataFrame = {
    val entity = Entity.load(entityname).get

    var res = expr.evaluate()

    if (nnq.isDefined) {
      res = FeatureScanner(Entity.load(entityname).get, nnq.get, Some(res))
    }

    if (withMetadata) {
      log.debug("join metadata to results of index query")
      val entity = Entity.load(entityname).get
      var data = entity.data
      var pk = entity.pk

      res = res.join(data, pk.name)
    }


    if (filter.isDefined) {
      res.join(filter.get, entity.pk.name)
    }

    run = true
    res
  }

  /**
    *
    * @return
    */
  def provideRunInfo(): Seq[RunDetails] = {
    if (!run) {
      log.warn("please run compound query before collecting run information")
      return Seq()
    } else {
      val start = getRunDetails(new ListBuffer())
      expr.getRunDetails(start).toSeq
    }
  }

  /**
    *
    * @param info
    * @return
    */
  override private[adam] def getRunDetails(info: ListBuffer[RunDetails]) = {
    super.getRunDetails(info)
  }
}
