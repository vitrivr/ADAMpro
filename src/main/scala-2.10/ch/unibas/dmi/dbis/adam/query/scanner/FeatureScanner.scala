package ch.unibas.dmi.dbis.adam.query.scanner

import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.entity.Tuple.TupleID
import ch.unibas.dmi.dbis.adam.entity.{Entity, Tuple}
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.Result
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable.ListBuffer

/**
  * adamtwo
  *
  * Scanner for the feature data.
  *
  * Ivan Giangreco
  * August 2015
  */
object FeatureScanner {
  val log = Logger.getLogger(getClass.getName)

  /**
    * Scans the feature data based on a nearest neighbour query.
    *
    * @param entity
    * @param query
    * @param filter if filter is defined, we pre-filter for the features
    * @return
    */
  def apply(entity: Entity, query: NearestNeighbourQuery, filter: Option[Set[TupleID]])(implicit ac : AdamContext): DataFrame = {
    val data = if (filter.isDefined) {
      //scan based on tuples filtered in index
      log.debug("scan features with pre-filter")
      ac.sc.setLocalProperty("spark.scheduler.pool", "feature")
      ac.sc.setJobGroup(query.queryID.getOrElse(""), entity.entityname, true)
      entity.filter(filter.get).collect()
    } else {
      //sequential scan
      log.debug("scan features without pre-filter")
      ac.sc.setLocalProperty("spark.scheduler.pool", "slow")
      ac.sc.setJobGroup(query.queryID.getOrElse(""), entity.entityname, true)
      entity.getFeaturedata.map(row => (row: Tuple)).collect()
    }

    val it = data.par.iterator

    //compute distance for candidates
    val ls = ListBuffer[Result]()
    while (it.hasNext) {
      val tuple = it.next
      val f: FeatureVector = tuple.feature
      ls += Result(query.distance(query.q, f), tuple.tid)
    }

    //kNN
    val result: ListBuffer[Result] = ls.sortBy(_.distance).take(query.k)

    //to DF
    val rdd = ac.sc.parallelize(result).map(res => Row(res.distance, res.tid))
    ac.sqlContext.createDataFrame(rdd, Result.resultSchema)
  }
}
