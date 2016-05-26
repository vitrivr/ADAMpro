package ch.unibas.dmi.dbis.adam.query.handler.internal

import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.Result
import ch.unibas.dmi.dbis.adam.query.datastructures.{QueryExpression, QueryCacheOptions}
import ch.unibas.dmi.dbis.adam.query.handler.BooleanQueryHandler
import ch.unibas.dmi.dbis.adam.query.query.BooleanQuery
import org.apache.spark.sql.{Row, DataFrame}

/**
  * adamtwo
  *
  * Ivan Giangreco
  * May 2016
  */
case class BooleanQueryHolder(entityname: EntityName)(bq: Option[BooleanQuery], id: Option[String] = None, cache: Option[QueryCacheOptions] = Some(QueryCacheOptions())) extends QueryExpression(id) {
  override protected def run(input: Option[DataFrame])(implicit ac: AdamContext): DataFrame = {
    var res = query(entityname)(bq, id, cache)

    if(input.isDefined){
      res.join(input.get, Entity.load(entityname).get.pk.name)
    }

    res
  }

  /**
    * Performs a Boolean query on the structured metadata.
    *
    * @param entityname
    * @param bq information for boolean query, if not specified all data is returned
    * @param id
    * @param cache
    */
  def query(entityname: EntityName)(bq: Option[BooleanQuery], id: Option[String] = None, cache: Option[QueryCacheOptions] = Some(QueryCacheOptions()))(implicit ac: AdamContext): DataFrame = {
    val entity = Entity.load(entityname).get
    var data = entity.metaData.getOrElse(entity.data.get)

    val res = if (bq.isDefined) {
      Some(BooleanQueryHandler.filter(data, bq.get))
    } else {
      None
    }

    if (res.isDefined) {
      import org.apache.spark.sql.functions._
      return res.get.withColumn(FieldNames.distanceColumnName, lit(0.toFloat))
    } else {
      val rdd = ac.sc.emptyRDD[Row]
      return ac.sqlContext.createDataFrame(rdd, Result.resultSchema(entity.pk.name))
    }
  }
}
