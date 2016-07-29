package ch.unibas.dmi.dbis.adam.index.structures.pq

import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature.FeatureVector
import ch.unibas.dmi.dbis.adam.datatypes.feature.FeatureVectorWrapper
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.index.Index.{IndexName, IndexTypeName}
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.distance.{DistanceFunction, MinkowskiDistance}
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.DataTypes

/**
  * adampro
  *
  * Ivan Giangreco
  * April 2016
  */
class PQIndex(val indexname: IndexName, val entityname: EntityName, override private[index] var data : DataFrame, private[index] val metadata: PQIndexMetaData)(@transient override implicit val ac : AdamContext)
  extends Index {

  override val indextypename: IndexTypeName = IndexTypes.PQINDEX

  override val lossy: Boolean = true
  override val confidence: Float = 0.5.toFloat


  override def scan(data : DataFrame, q : FeatureVector, distance : DistanceFunction, options : Map[String, String], k : Int): DataFrame = {
    log.debug("scanning PQ index " + indexname)

    //precompute distance
    val distances = ac.sc.broadcast(q.toArray
      .grouped(math.max(1, q.size / metadata.nsq)).toSeq
      .zipWithIndex
      .map { case (split, idx) =>
        val qsub = new FeatureVectorWrapper(split)
        metadata.models(idx).clusterCenters.map(center => {
          distance(new FeatureVectorWrapper(center.toArray.map(_.toFloat)).vector, qsub.vector)
        }).toIndexedSeq
      }.toIndexedSeq)


    import org.apache.spark.sql.functions.udf
    val distUDF = udf((c: Seq[Byte]) => {
      var i : Int = 0
      var sum : Float = 0
      //sum up distance of each part by choosing the right cluster
      while(i < c.length){
        sum += distances.value(i)(c(i))
        i += 1
      }
      sum
    })

    data
      .withColumn(FieldNames.distanceColumnName, distUDF(data(FieldNames.featureIndexColumnName)).cast(DataTypes.FloatType))
      .sort(FieldNames.distanceColumnName)
      .limit(k)
  }

  override def isQueryConform(nnq: NearestNeighbourQuery): Boolean = {
    //is this check correct?
    if (nnq.distance.isInstanceOf[MinkowskiDistance]) {
      return true
    }

    false
  }
}


object PQIndex {
  def apply(indexname: IndexName, entityname: EntityName, data: DataFrame, meta: Any)(implicit ac : AdamContext): PQIndex = {
    val indexMetaData = meta.asInstanceOf[PQIndexMetaData]
    new PQIndex(indexname, entityname, data, indexMetaData)
  }
}