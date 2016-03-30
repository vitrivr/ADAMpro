package ch.unibas.dmi.dbis.adam.index.structures.ecp

import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.index.Index.{IndexName, IndexTypeName}
import ch.unibas.dmi.dbis.adam.index._
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.query.distance.DistanceFunction
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.util.random.ADAMSamplingUtils

/**
 * adamtwo
 *
 * Ivan Giangreco
 * October 2015
 */
class ECPIndexer(distance : DistanceFunction) extends IndexGenerator with Serializable {
  @transient lazy val log = Logger.getLogger(getClass.getName)

  override val indextypename: IndexTypeName = IndexTypes.ECPINDEX

  /**
   *
   * @param indexname
   * @param entityname
   * @param data
   * @return
   */
  override def index(indexname: IndexName, entityname: EntityName, data: RDD[IndexingTaskTuple]): Index = {
    val n = Entity.countTuples(entityname)
    val trainingSize = math.sqrt(n)
    val fraction = ADAMSamplingUtils.computeFractionForSampleSize(trainingSize.toInt, n, false)
    val trainData = data.sample(false, fraction)

    val leaders = data.sample(true, fraction).collect
    val broadcastLeaders = SparkStartup.sc.broadcast(leaders)
    log.debug("eCP index leaders chosen and broadcasted")

    log.debug("eCP indexing...")

    val indexdata = data.map(datum => {
        val minTID = broadcastLeaders.value.map({ l =>
          (l.id, distance.apply(datum.feature, l.feature))
        }).minBy(_._2)._1

        LongIndexTuple(datum.id, minTID)
      })

    import SparkStartup.sqlContext.implicits._
    new ECPIndex(indexname, entityname, indexdata.toDF, ECPIndexMetaData(leaders.toArray.toSeq, distance))
  }
}

object ECPIndexer {
  def apply(distance : DistanceFunction) : IndexGenerator = new ECPIndexer(distance)
}