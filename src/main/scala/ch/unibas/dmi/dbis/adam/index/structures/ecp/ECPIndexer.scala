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
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.util.random.Sampling

/**
  * adamtwo
  *
  * Ivan Giangreco
  * October 2015
  */
class ECPIndexer(centroidBasedLeaders: Boolean, distance: DistanceFunction, trainingSize: Option[Int])(@transient implicit val ac: AdamContext) extends IndexGenerator {
  override val indextypename: IndexTypeName = IndexTypes.ECPINDEX

  /**
    *
    * @param indexname  name of index
    * @param entityname name of entity
    * @param data       data to index
    * @return
    */
  override def index(indexname: IndexName, entityname: EntityName, data: RDD[IndexingTaskTuple[_]]): Index = {
    val entity = Entity.load(entityname).get


    //randomly choose leaders
    val n = entity.count
    val fraction = Sampling.computeFractionForSampleSize(math.max(trainingSize.getOrElse(math.sqrt(n).toInt), IndexGenerator.MINIMUM_NUMBER_OF_TUPLE), n, withReplacement = false)
    var trainData = data.sample(false, fraction).collect()
    if (trainData.length < IndexGenerator.MINIMUM_NUMBER_OF_TUPLE) {
      trainData = data.take(IndexGenerator.MINIMUM_NUMBER_OF_TUPLE)
    }

    val bcleaders = ac.sc.broadcast(trainData.zipWithIndex.map { case (idt, idx) => IndexingTaskTuple(idx, idt.feature) }) //use own ids, not id of data
    log.trace("eCP index chosen " + trainData.length + " leaders")

    log.debug("eCP indexing...")

    val indexdata = data.map(datum => {
      val minTID = bcleaders.value.map({ l =>
        (l.id, distance.apply(datum.feature, l.feature))
      }).minBy(_._2)._1

      (datum.id, minTID, datum.feature)
    })

    val schema = StructType(Seq(
      StructField(entity.pk.name, entity.pk.fieldtype.datatype, nullable = false),
      StructField(FieldNames.featureIndexColumnName, IntegerType, nullable = false)
    ))

    val df = ac.sqlContext.createDataFrame(indexdata.map(x => Row(x._1, x._2)), schema)

    log.trace("eCP index updating leaders")

    val leaders = if (centroidBasedLeaders) {
      //compute centroid
      indexdata.groupBy(_._2).map { case (id, data) =>
        val vectors = data.map(x => x._3).toSeq
        val newLeader = vectors.reduce((x, y) => x + y)./=(vectors.length.toFloat)
        IndexingTaskTuple(id, newLeader)
      }.collect()
    } else {
      //use feature vector chosen in beginning as leader
      bcleaders.value
    }

    new ECPIndex(indexname, entityname, df, ECPIndexMetaData(leaders.toSeq, distance))
  }
}

object ECPIndexer {
  /**
    *
    * @param distance   distance function
    * @param properties indexing properties
    * @return
    */
  def apply(distance: DistanceFunction, properties: Map[String, String] = Map[String, String]())(implicit ac: AdamContext): IndexGenerator = {
    val trainingSize = properties.get("ntraining").map(_.toInt)

    val leaderTypeDescription = properties.getOrElse("leadertype", "simple")
    val leaderType = leaderTypeDescription.toLowerCase match {
      //possibly extend with other types and introduce enum
      case "centroid" => true
      case "simple" => false
    }

    new ECPIndexer(leaderType, distance, trainingSize)
  }
}