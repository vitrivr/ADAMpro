package ch.unibas.dmi.dbis.adam.index.structures.pq

import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature.FeatureVector
import ch.unibas.dmi.dbis.adam.datatypes.feature.FeatureVectorWrapper
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.index.{ByteArrayIndexTuple, Index}
import ch.unibas.dmi.dbis.adam.query.Result
import ch.unibas.dmi.dbis.adam.query.distance.{DistanceFunction, MinkowskiDistance}
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery
import org.apache.spark.sql.DataFrame

/**
  * adampro
  *
  * Ivan Giangreco
  * April 2016
  */
class PQIndex(val indexname: IndexName, val entityname: EntityName, protected val df: DataFrame, private[index] val metadata: PQIndexMetaData)
  extends Index {
  override val indextype: IndexTypeName = IndexTypes.PQINDEX

  override def scan(data : DataFrame, q : FeatureVector, distance : DistanceFunction, options : Map[String, Any], k : Int): Set[Result] = {
    log.debug("scanning PQ index " + indexname)

    //precompute distance
    val distances = q.toArray
      .grouped(q.size / metadata.nsq).toSeq
      .zipWithIndex
      .map { case (split, idx) => {
        val qsub = new FeatureVectorWrapper(split)
        metadata.models(idx).clusterCenters.map(center => {
          distance(new FeatureVectorWrapper(center.toArray.map(_.toFloat)).vector, qsub.vector)
        }).toIndexedSeq
      }
      }.toIndexedSeq


    val rdd = data.map(r => r: ByteArrayIndexTuple)

    val ids = rdd.map{tuple =>
      val sum = tuple.value.zipWithIndex.map { case (value, idx) => distances(idx)(value) }.sum
      Result(sum, tuple.id)
    }.takeOrdered(k)

    log.debug("PQ index returning " + ids.length + " tuples")

    ids.toSet
  }

  override def isQueryConform(nnq: NearestNeighbourQuery): Boolean = {
    //is this check correct?
    if (nnq.distance.isInstanceOf[MinkowskiDistance]) {
      return true
    }

    false
  }

  override val confidence: Float = 0.toFloat
  override val lossy: Boolean = true
}


object PQIndex {
  def apply(indexname: IndexName, entityname: EntityName, data: DataFrame, meta: Any): PQIndex = {
    val indexMetaData = meta.asInstanceOf[PQIndexMetaData]
    new PQIndex(indexname, entityname, data, indexMetaData)
  }
}