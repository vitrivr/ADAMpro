package ch.unibas.dmi.dbis.adam.index.structures.ecp

import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature.FeatureVector
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.index.Index.{IndexName, IndexTypeName}
import ch.unibas.dmi.dbis.adam.index._
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.Result
import ch.unibas.dmi.dbis.adam.query.distance.DistanceFunction
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ListBuffer

/**
  * adamtwo
  *
  * Ivan Giangreco
  * October 2015
  */
class ECPIndex(val indexname: IndexName, val entityname: EntityName, override private[index] var data : DataFrame, private[index] val metadata: ECPIndexMetaData)(@transient implicit val ac : AdamContext)
  extends Index {

  override val indextypename: IndexTypeName = IndexTypes.ECPINDEX

  override val lossy: Boolean = true
  override val confidence = 0.toFloat

  override def scan(data: DataFrame, q: FeatureVector, distance: DistanceFunction, options: Map[String, Any], k: Int): Set[Result] = {
    log.debug("scanning eCP index " + indexname)

    val centroids = ac.sc.broadcast(metadata.leaders.map(l => {
      (l.id, metadata.distance(q, l.feature))
    }).sortBy(_._2))


    var results = ListBuffer[Result]()
    var i = 0
    while (i < centroids.value.length && results.length < k) {
      results ++= data.filter(data(FieldNames.featureIndexColumnName) === centroids.value(i)._1).collect()
        .map(tuple => Result(centroids.value(i)._2, tuple.getAs[Any](this.pk.name)))
      i += 1
    }
    val ids = results.toSeq

    log.debug("eCP index returning " + ids.length + " tuples")

    ids.toSet
  }

  override def isQueryConform(nnq: NearestNeighbourQuery): Boolean = true
}

object ECPIndex {
  def apply(indexname: IndexName, tablename: EntityName, data: DataFrame, meta: Any)(implicit ac : AdamContext): ECPIndex = {
    val indexMetaData = meta.asInstanceOf[ECPIndexMetaData]
    new ECPIndex(indexname, tablename, data, indexMetaData)
  }
}