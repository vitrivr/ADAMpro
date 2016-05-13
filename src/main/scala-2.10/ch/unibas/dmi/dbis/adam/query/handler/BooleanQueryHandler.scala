package ch.unibas.dmi.dbis.adam.query.handler

import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.main.{AdamContext, SparkStartup}
import ch.unibas.dmi.dbis.adam.query.query.{BooleanQuery, PrimaryKeyFilter}
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

/**
  * adamtwo
  *
  * Ivan Giangreco
  * November 2015
  */
private[query] object BooleanQueryHandler {
  val log = Logger.getLogger(getClass.getName)


  /**
    * Creates a filter that is applied on the nearest neighbour search based on the Boolean query.
    *
    * @param entityname
    * @param bq
    * @return
    */
  def getFilter(entityname: EntityName, bq: Option[BooleanQuery], tiq: Option[PrimaryKeyFilter[_]])(implicit ac: AdamContext): Option[DataFrame] = {
    if (bq.isEmpty && tiq.isEmpty) {
      return None
    }

    val entity = Entity.load(entityname).get
    var data = entity.data
    var pk = entity.pk


    if (bq.isDefined) {
      data = BooleanQueryHandler.filter(data, bq.get)
    }

    if (tiq.isDefined) {
      data = BooleanQueryHandler.filter(data, pk.name, tiq.get)
    }

    Some(data.select(pk.name))
  }

  /**
    *
    * @param data
    * @param query
    * @param ac
    * @return
    */
  def filter(data : DataFrame, query: BooleanQuery)(implicit ac: AdamContext): DataFrame = {
    var df : DataFrame = data

    if (query.join.isDefined) {
      log.debug("join tables to results")
      val joins = query.join.get

      for (i <- (0 until joins.length)) {
        val join = joins(i)
        log.debug("join " + join._1 + " on " + join._2.mkString("(", ", ", ")"))
        val newDF = SparkStartup.metadataStorage.read(join._1)
        df = df.join(newDF, join._2)
      }
    }

    if (query.where.isDefined) {
      val where = query.buildWhereClause()
      log.debug("query metadata using where clause: " + where)
      df = df.filter(where)
    }

    df
  }

  /**
    *
    * @param data
    * @param pk
    * @param query
    * @param ac
    * @return
    */
  def filter(data : DataFrame, pk : String, query: PrimaryKeyFilter[_])(implicit ac: AdamContext): DataFrame = {
    import org.apache.spark.sql.functions.col
    data.filter(col(pk).isin(query.tidFilter.toSeq: _*))
  }
}
