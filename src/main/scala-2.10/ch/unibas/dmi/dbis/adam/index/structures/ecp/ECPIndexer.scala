package ch.unibas.dmi.dbis.adam.index.structures.ecp

import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.entity.EntityHandler
import ch.unibas.dmi.dbis.adam.index.Index.{IndexName, IndexTypeName}
import ch.unibas.dmi.dbis.adam.index._
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.main.{AdamContext, SparkStartup}
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
class ECPIndexer(trainingSize: Int = -1, distance: DistanceFunction)(@transient implicit val ac : AdamContext) extends IndexGenerator with Serializable {
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
    val n = EntityHandler.countTuples(entityname).get
    val ntuples = if (trainingSize == -1) {
      math.sqrt(n)
    } else {
      trainingSize
    }

    val fraction = ADAMSamplingUtils.computeFractionForSampleSize(ntuples.toInt, n, false)

    val leaders = ac.sc.broadcast(data.sample(true, fraction).collect)
    log.debug("eCP index leaders chosen and broadcasted")

    log.debug("eCP indexing...")

    val indexdata = data.map(datum => {
      val minTID = leaders.value.map({ l =>
        (l.id, distance.apply(datum.feature, l.feature))
      }).minBy(_._2)._1

      LongIndexTuple(datum.id, minTID)
    })

    import SparkStartup.Implicits.sqlContext.implicits._
    new ECPIndex(indexname, entityname, indexdata.toDF, ECPIndexMetaData(leaders.value.toArray.toSeq, distance))
  }
}

object ECPIndexer {
  def apply(distance: DistanceFunction, properties : Map[String, String] = Map[String, String]())(implicit ac : AdamContext): IndexGenerator = {
    val trainingSize = properties.getOrElse("ntraining", "-1").toInt
    new ECPIndexer(trainingSize, distance)
  }
}