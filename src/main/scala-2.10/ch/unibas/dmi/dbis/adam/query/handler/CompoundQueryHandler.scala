package ch.unibas.dmi.dbis.adam.query.handler

import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.entity.EntityHandler
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.Result
import ch.unibas.dmi.dbis.adam.query.datastructures.QueryExpression
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery
import ch.unibas.dmi.dbis.adam.query.scanner.FeatureScanner
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Row}

/**
  * adampro
  *
  * Ivan Giangreco
  * March 2016
  */
private[query] object CompoundQueryHandler {
  val log = Logger.getLogger(getClass.getName)

  /**
    *
    * @param entityname
    * @param nnq
    * @param expr
    * @param withMetadata
    * @return
    */
  def indexQueryWithResults(entityname: EntityName)(nnq: NearestNeighbourQuery, expr: QueryExpression, withMetadata: Boolean)(implicit ac: AdamContext): DataFrame = {
    val tidFilter = expr.evaluate()
    var res = FeatureScanner(EntityHandler.load(entityname).get, nnq, Some(tidFilter))

    if (withMetadata) {
      log.debug("join metadata to results of compound query")
      res = QueryHandler.joinWithMetadata(entityname, res)
    }

    res
  }

  /**
    *
    * @param entityname
    * @param expr
    * @param withMetadata
    * @param ac
    * @return
    */
  def indexOnlyQuery(entityname: EntityName)(expr: QueryExpression, withMetadata: Boolean = false)(implicit ac: AdamContext): DataFrame = {
    val entity = EntityHandler.load(entityname).get

    val tidFilter = expr.evaluate().map(x => Result(0.toFloat, x.getAs[Long](entity.pk.name))).map(_.tid).collect().toSet

    val rdd = ac.sc.parallelize(tidFilter.toSeq).map(res => Row(0.toFloat, res))
    var res = ac.sqlContext.createDataFrame(rdd, Result.resultSchema(entity.pk.name))

    if (withMetadata) {
      log.debug("join metadata to results of compound query")
      res = QueryHandler.joinWithMetadata(entityname, res)
    }

    res
  }


}
