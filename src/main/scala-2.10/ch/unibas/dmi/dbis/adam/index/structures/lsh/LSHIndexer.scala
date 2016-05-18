package ch.unibas.dmi.dbis.adam.index.structures.lsh

import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.datatypes.bitString.BitStringUDT
import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.index.Index.{IndexName, IndexTypeName}
import ch.unibas.dmi.dbis.adam.index._
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.index.structures.lsh.hashfunction.{EuclideanHashFunction, Hasher, ManhattanHashFunction}
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.distance.{DistanceFunction, EuclideanDistance, ManhattanDistance}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.util.random.ADAMSamplingUtils


class LSHIndexer(numHashTables: Int, numHashes: Int, distance: DistanceFunction, trainingSize: Int)(@transient implicit val ac: AdamContext) extends IndexGenerator {
  override val indextypename: IndexTypeName = IndexTypes.LSHINDEX

  /**
    *
    * @param indexname
    * @param entityname
    * @param data
    * @return
    */
  override def index(indexname: IndexName, entityname: EntityName, data: RDD[IndexingTaskTuple[_]]): Index = {
    val entity = Entity.load(entityname).get

    val n = entity.count
    val fraction = ADAMSamplingUtils.computeFractionForSampleSize(trainingSize, n, false)
    val trainData = data.sample(false, fraction)

    val indexMetaData = train(trainData.collect())

    log.debug("LSH indexing...")

    val indexdata = data.map(
      datum => {
        val hash = LSHUtils.hashFeature(datum.feature, indexMetaData)
        Row(datum.id, hash)
      })

    val schema = StructType(Seq(
      StructField(entity.pk.name, entity.pk.fieldtype.datatype, false),
      StructField(FieldNames.featureIndexColumnName, new BitStringUDT, false)
    ))

    val df = ac.sqlContext.createDataFrame(indexdata, schema)
    new LSHIndex(indexname, entityname, df, indexMetaData)
  }

  /**
    *
    * @param trainData
    * @return
    */
  private def train(trainData: Array[IndexingTaskTuple[_]]): LSHIndexMetaData = {
    log.debug("LSH started training")

    //data
    val dims = trainData.head.feature.size

    val radiuses = {
      val res = for (a <- trainData; b <- trainData) yield (a.id, distance(a.feature, b.feature))
      res.groupBy(_._1).map(x => x._2.map(_._2).max)
    }.toSeq
    val radius = radiuses.sum / radiuses.length

    val hashFamily = distance match {
      case ManhattanDistance => () => new ManhattanHashFunction(dims, radius.toFloat, 256)
      case EuclideanDistance => () => new EuclideanHashFunction(dims, radius.toFloat, 256)
      case _ => null
    }
    val hashTables = (0 until numHashTables).map(i => new Hasher(hashFamily, numHashes))

    log.debug("LSH finished training")

    LSHIndexMetaData(hashTables, radius.toFloat, distance)
  }
}


object LSHIndexer {
  /**
    *
    * @param properties
    */
  def apply(distance: DistanceFunction, properties: Map[String, String] = Map[String, String]())(implicit ac: AdamContext): IndexGenerator = {
    val numHashTables = properties.getOrElse("nhashtables", "64").toInt
    val numHashes = properties.getOrElse("nhashes", "64").toInt

    val norm = properties.getOrElse("norm", "2").toInt

    val trainingSize = properties.getOrElse("ntraining", "500").toInt

    new LSHIndexer(numHashTables, numHashes, distance, trainingSize)
  }
}
