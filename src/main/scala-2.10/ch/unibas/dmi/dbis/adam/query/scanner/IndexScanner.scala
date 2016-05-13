package ch.unibas.dmi.dbis.adam.query.scanner

import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.Result
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Row}


/**
  * adamtwo
  *
  * Performs an index scan.
  *
  * Ivan Giangreco
  * August 2015
  */
object IndexScanner {
  val log = Logger.getLogger(getClass.getName)

  /**
    * Performs an index scan.
    *
    * @param index
    * @param query
    * @param filter pre-filter to use when scanning the index
    * @return
    */
  def apply(index: Index, query: NearestNeighbourQuery, filter: Option[DataFrame])(implicit ac : AdamContext): DataFrame = {
    log.debug("scan index")
    val results = index.scan(query.q, query.distance, query.options, query.k, filter, query.partitions, query.queryID)
    val rdd = ac.sc.parallelize(results.map(result => Row(result.distance, result.tid)).toSeq)
    val fields = Result.resultSchema(index.pk.name)
    ac.sqlContext.createDataFrame(rdd, fields)
  }
}