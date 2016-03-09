package ch.unibas.dmi.dbis.adam.query.scanner

import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.entity.{Tuple, Entity}
import ch.unibas.dmi.dbis.adam.entity.Tuple.TupleID
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.query.Result
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery

import scala.collection.immutable.HashSet
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
  /**
    * Scans the feature data based on a nearest neighbour query.
    *
    * @param entity
    * @param query
    * @param filter if filter is defined, we pre-filter for the features
    * @return
    */
  def apply(entity: Entity, query: NearestNeighbourQuery, filter: Option[HashSet[TupleID]]): Seq[Result] = {
    val data = if (filter.isDefined) {
      //scan based on tuples filtered in index
      SparkStartup.sc.setLocalProperty("spark.scheduler.pool", "feature")
      SparkStartup.sc.setJobGroup(query.queryID.getOrElse(""), entity.entityname, true)
      entity.filter(filter.get).collect()
    } else {
      //sequential scan
      SparkStartup.sc.setLocalProperty("spark.scheduler.pool", "slow")
      SparkStartup.sc.setJobGroup(query.queryID.getOrElse(""), entity.entityname, true)
      entity.rdd.map(row => (row: Tuple)).collect()
    }

    val it = data.par.iterator

    //compute distance for candidates
    val ls = ListBuffer[Result]()
    while (it.hasNext) {
      val tuple = it.next
      val f: FeatureVector = tuple.feature
      ls += Result(query.distance(query.q, f), tuple.tid, null)
    }

    //kNN
    ls.sortBy(_.distance).take(query.k)
  }
}
