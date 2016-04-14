package ch.unibas.dmi.dbis.adam.query.handler

import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.query.Result
import ch.unibas.dmi.dbis.adam.query.datastructures.QueryExpression
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery
import ch.unibas.dmi.dbis.adam.query.scanner.FeatureScanner
import org.apache.spark.sql.{DataFrame, Row}

/**
  * adampro
  *
  * Ivan Giangreco
  * March 2016
  */
private[query] object CompoundQueryHandler {

  /**
    *
    * @param entityname
    * @param nnq
    * @param expr
    * @param withMetadata
    * @return
    */
  def indexQueryWithResults(entityname: EntityName)(nnq: NearestNeighbourQuery, expr: QueryExpression, withMetadata: Boolean): DataFrame = {
    val tidFilter = expr.evaluate().map(x => Result(0.toFloat, x.getAs[Long](FieldNames.idColumnName))).map(_.tid).collect().toSet
    var res = FeatureScanner(Entity.load(entityname).get, nnq, Some(tidFilter))

    if (withMetadata) {
      log.debug("join metadata to results of compound query")
      res = QueryHandler.joinWithMetadata(entityname, res)
    }

    res
  }

  /**
    *
    * @param expr
    * @return
    */
  def indexOnlyQuery(entityname: EntityName = "")(expr: QueryExpression, withMetadata: Boolean = false): DataFrame = {
    val tidFilter = expr.evaluate().map(x => Result(0.toFloat, x.getAs[Long](FieldNames.idColumnName))).map(_.tid).collect().toSet

    val rdd = SparkStartup.sc.parallelize(tidFilter.toSeq).map(res => Row(0.toFloat, res))
    var res = SparkStartup.sqlContext.createDataFrame(rdd, Result.resultSchema)

    if (withMetadata) {
      log.debug("join metadata to results of compound query")
      res = QueryHandler.joinWithMetadata(entityname, res)
    }

   res
  }




}
