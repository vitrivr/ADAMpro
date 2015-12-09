package ch.unibas.dmi.dbis.adam.index.structures.ecp

import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.index.Index.{IndexName, IndexTypeName}
import ch.unibas.dmi.dbis.adam.index._
import ch.unibas.dmi.dbis.adam.index.structures.IndexStructures
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.query.distance.DistanceFunction
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import org.apache.spark.rdd.RDD
import org.apache.spark.util.random.ADAMSamplingUtils

/**
 * adamtwo
 *
 * Ivan Giangreco
 * October 2015
 */
class ECPIndexer(distance : DistanceFunction) extends IndexGenerator with Serializable {
  override val indextypename: IndexTypeName = IndexStructures.ECP

  /**
   *
   * @param indexname
   * @param entityname
   * @param data
   * @return
   */
  override def index(indexname: IndexName, entityname: EntityName, data: RDD[IndexerTuple]): Index[_ <: IndexTuple] = {
    val n = Entity.countEntity(entityname)
    val trainingSize = math.sqrt(n)
    val fraction = ADAMSamplingUtils.computeFractionForSampleSize(trainingSize.toInt, n, false)
    val trainData = data.sample(false, fraction)

    val leaders = data.sample(true, fraction).collect
    val broadcastLeaders = SparkStartup.sc.broadcast(leaders)

    val indexdata = data.map(datum => {
        val minTID = broadcastLeaders.value.map({ l =>
          (l.tid, distance.apply(datum.value, l.value))
        }).minBy(_._2)._1

        LongIndexTuple(datum.tid, minTID)
      })

    import SparkStartup.sqlContext.implicits._
    new ECPIndex(indexname, entityname, indexdata.toDF, ECPIndexMetaData(leaders.toArray.toSeq, distance))
  }
}

object ECPIndexer {
  def apply(properties : Map[String, String] = Map[String, String](), distance : DistanceFunction, data: RDD[IndexerTuple]) : IndexGenerator = new ECPIndexer(distance)
}