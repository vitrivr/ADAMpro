package ch.unibas.dmi.dbis.adam.query.handler.internal

import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.datastructures.{QueryCacheOptions, QueryExpression}
import ch.unibas.dmi.dbis.adam.query.handler.BooleanQueryHandler
import ch.unibas.dmi.dbis.adam.query.progressive.{ProgressiveQueryHandler, ProgressivePathChooser}
import ch.unibas.dmi.dbis.adam.query.query.{BooleanQuery, NearestNeighbourQuery, PrimaryKeyFilter}
import org.apache.spark.sql.DataFrame

import scala.concurrent.duration.Duration

/**
  * adamtwo
  *
  * Ivan Giangreco
  * May 2016
  */
class TimedProgressiveQueryHolder(entityname: EntityName,
  nnq: NearestNeighbourQuery, bq: Option[BooleanQuery], tiq: Option[PrimaryKeyFilter[_]],
  paths: ProgressivePathChooser,
  timelimit: Duration, withMetadata: Boolean, id: Option[String] = None, cache: Option[QueryCacheOptions] = Some(QueryCacheOptions())) extends QueryExpression(id) {
  /**
    *
    * @param input
    * @return
    */
  override protected def run(input: Option[DataFrame])(implicit ac: AdamContext): DataFrame = {
    val filter = BooleanQueryHandler.getFilter(entityname, bq, tiq)
    ProgressiveQueryHandler.timedProgressiveQuery(entityname)(nnq, bq, tiq, paths, timelimit, withMetadata, id, cache)._1
  }
}
