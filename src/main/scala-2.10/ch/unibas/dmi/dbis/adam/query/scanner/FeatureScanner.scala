package ch.unibas.dmi.dbis.adam.query.scanner

import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Tuple.TupleID
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.query.Result
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery

import scala.collection.immutable.HashSet
import scala.collection.mutable.ListBuffer

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
object FeatureScanner {
  def apply(entity : Entity, query : NearestNeighbourQuery, filter: Option[HashSet[TupleID]]): Seq[Result] = {
    SparkStartup.sc.setLocalProperty("spark.scheduler.pool", "feature")
    SparkStartup.sc.setJobGroup(query.queryID.getOrElse(""), entity.entityname, true)

    val data = if(filter.isDefined) {
      entity.featuresForKeys(filter.get).collect()
    } else {
      entity.featuresTuples.collect()
    }

    val it = data.par.iterator

    val ls = ListBuffer[Result]()
    while(it.hasNext){
      val tuple = it.next
      val f : FeatureVector = tuple.value
      ls += Result(query.distance(query.q, f), tuple.tid, null)
    }

    ls.sortBy(_.distance).take(query.k)
  }
}
