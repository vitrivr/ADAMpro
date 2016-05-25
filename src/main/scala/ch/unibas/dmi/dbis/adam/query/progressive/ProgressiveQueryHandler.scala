package ch.unibas.dmi.dbis.adam.query.progressive

import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.datastructures._
import ch.unibas.dmi.dbis.adam.query.handler.{BooleanQueryHandler, NearestNeighbourQueryHandler}
import ch.unibas.dmi.dbis.adam.query.query.{BooleanQuery, NearestNeighbourQuery, PrimaryKeyFilter}
import org.apache.spark.Logging
import org.apache.spark.sql.DataFrame

import scala.concurrent.duration.Duration

/**
  * adamtwo
  *
  * Ivan Giangreco
  * November 2015
  */
object ProgressiveQueryHandler extends Logging {
  //TODO: generalize

  /**
    * Performs a progressive query, i.e., all indexes and sequential search are started at the same time and results are returned as soon
    * as they are available. When a precise result is returned, the whole query is stopped.
    *
    * @param entityname
    * @param nnq          information for nearest neighbour query
    * @param bq           information for boolean query
    * @param onComplete   operation to perform as soon as one index returns results
    *                     (the operation takes parameters:
    *                     - status value
    *                     - result (as DataFrame)
    *                     - confidence score denoting how confident you can be in the results
    *                     - further information (key-value)
    * @param withMetadata whether or not to retrieve corresponding metadata
    * @return a tracker for the progressive query
    */
  def progressiveQuery[U](entityname: EntityName)(
    nnq: NearestNeighbourQuery, bq: Option[BooleanQuery], tiq: Option[PrimaryKeyFilter[_]],
    paths: ProgressivePathChooser,
    onComplete: (ProgressiveQueryStatus.Value, DataFrame, Float, String, Map[String, String]) => U,
    withMetadata: Boolean, id: Option[String] = None)(implicit ac: AdamContext)
  : ProgressiveQueryStatusTracker = {
    //TODO: use cache

    val onCompleteFunction = if (withMetadata) {
      log.debug("join metadata to results of progressive query")
      (pqs: ProgressiveQueryStatus.Value, res: DataFrame, conf: Float, source: String, info: Map[String, String]) => onComplete(pqs, joinWithMetadata(entityname, res), conf, source, info)
    } else {
      onComplete
    }

    log.debug("progressive query gets filter")
    val filter = BooleanQueryHandler.getFilter(entityname, bq, tiq)

    log.debug("progressive query performs kNN query")
    NearestNeighbourQueryHandler.progressiveQuery(entityname, nnq, filter, paths, onCompleteFunction)
  }

  /**
    * Performs a timed progressive query, i.e., it performs the query for a maximum of the given time limit and returns then the best possible
    * available results.
    *
    * @param entityname
    * @param nnq          information for nearest neighbour query
    * @param bq           information for boolean query
    * @param timelimit    maximum time to wait
    * @param withMetadata whether or not to retrieve corresponding metadata
    * @return the results available together with a confidence score
    */
  def timedProgressiveQuery(entityname: EntityName)(
    nnq: NearestNeighbourQuery, bq: Option[BooleanQuery], tiq: Option[PrimaryKeyFilter[_]],
    paths: ProgressivePathChooser,
    timelimit: Duration, withMetadata: Boolean, id: Option[String] = None, cache: Option[QueryCacheOptions] = Some(QueryCacheOptions()))(implicit ac: AdamContext): (DataFrame, Float, String) = {
    log.debug("timed progressive query gets filter")
    //TODO: use cache

    val filter = BooleanQueryHandler.getFilter(entityname, bq, tiq)

    log.debug("timed progressive query performs kNN query")
    val results = NearestNeighbourQueryHandler.timedProgressiveQuery(entityname, nnq, filter, paths, timelimit)
    var res = results.results

    if (withMetadata) {
      log.debug("join metadata to results of timed progressive query")
      res = joinWithMetadata(entityname, res)
    }

    (res, results.confidence, results.source)
  }





  private def joinWithMetadata(entityname: EntityName, res: DataFrame)(implicit ac: AdamContext) : DataFrame = {
    val entity = Entity.load(entityname).get
    var data = entity.data
    var pk = entity.pk

    res.join(data, pk.name)
  }
}
