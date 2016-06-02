package ch.unibas.dmi.dbis.adam.index.structures.ecp

import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.index.Index.{IndexName, IndexTypeName}
import ch.unibas.dmi.dbis.adam.index._
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.distance.DistanceFunction
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.util.random.ADAMSamplingUtils

/**
  * adamtwo
  *
  * Ivan Giangreco
  * October 2015
  */
class ECPIndexer(trainingSize: Option[Int], distance: DistanceFunction)(@transient implicit val ac: AdamContext) extends IndexGenerator {
  override val indextypename: IndexTypeName = IndexTypes.ECPINDEX

  /**
    *
    * @param indexname name of index
    * @param entityname name of entity
    * @param data data to index
    * @return
    */
  override def index(indexname: IndexName, entityname: EntityName, data: RDD[IndexingTaskTuple[_]]): Index = {
    val entity = Entity.load(entityname).get

    val n = entity.count
    val fraction = ADAMSamplingUtils.computeFractionForSampleSize(math.max(trainingSize.getOrElse(math.sqrt(n).toInt), IndexGenerator.MINIMUM_NUMBER_OF_TUPLE), n, withReplacement = false)
    var trainData = data.sample(false, fraction).collect()
    if(trainData.length < IndexGenerator.MINIMUM_NUMBER_OF_TUPLE){
      trainData = data.take(IndexGenerator.MINIMUM_NUMBER_OF_TUPLE)
    }

    val leaders = ac.sc.broadcast(trainData)
    log.trace("eCP index chosen " + trainData.length + " leaders")

    log.debug("eCP indexing...")

    val indexdata = data.map(datum => {
      val minTID = leaders.value.map({ l =>
        (l.id, distance.apply(datum.feature, l.feature))
      }).minBy(_._2)._1

      Row(datum.id, minTID)
    })

    val schema = StructType(Seq(
      StructField(entity.pk.name, entity.pk.fieldtype.datatype, nullable = false),
      StructField(FieldNames.featureIndexColumnName, entity.pk.fieldtype.datatype, nullable = false)
    ))

    val df = ac.sqlContext.createDataFrame(indexdata, schema)

    new ECPIndex(indexname, entityname, df, ECPIndexMetaData(leaders.value.toSeq, distance))
  }
}

object ECPIndexer {
  /**
    *
    * @param distance distance function
    * @param properties indexing properties
    * @return
    */
  def apply(distance: DistanceFunction, properties: Map[String, String] = Map[String, String]())(implicit ac: AdamContext): IndexGenerator = {
    val trainingSize = properties.get("ntraining").map(_.toInt)
    new ECPIndexer(trainingSize, distance)
  }
}