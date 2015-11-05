package ch.unibas.dmi.dbis.adam.query

import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.query.distance.DistanceFunction
import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Tuple.TupleID

import scala.collection.immutable.HashSet
import scala.collection.mutable.ListBuffer

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
object FeatureScanner {
  def apply(entity : Entity, q: FeatureVector, distance : DistanceFunction, k : Int, filter: Option[HashSet[TupleID]], queryID : Option[String] = None): Seq[Result] = {
    SparkStartup.sc.setLocalProperty("spark.scheduler.pool", "feature")
    SparkStartup.sc.setJobGroup(queryID.getOrElse(""), entity.entityname, true)

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
      ls += Result(distance(q, f), tuple.tid, null)
    }

    ls.sortBy(_.distance).take(k)
  }
}
