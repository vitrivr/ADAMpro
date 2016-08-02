package ch.unibas.dmi.dbis.adam.index.structures.lsh

import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.datatypes.bitString.BitString
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.datatypes.feature.MovableFeature
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.index.Index.{IndexName, IndexTypeName}
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.index.structures.lsh.signature.LSHSignatureGenerator
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.Result
import ch.unibas.dmi.dbis.adam.query.distance.DistanceFunction
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.functions.col


/**
  * adamtwo
  *
  * Ivan Giangreco
  * August 2015
  */
class LSHIndex(val indexname: IndexName, val entityname: EntityName, override private[index] var data: DataFrame, private[index] val metadata: LSHIndexMetaData)(@transient override implicit val ac: AdamContext)
  extends Index {

  override val indextypename: IndexTypeName = IndexTypes.LSHINDEX

  override val lossy: Boolean = true
  override val confidence = 0.5.toFloat

  override def scan(data: DataFrame, q: FeatureVector, distance: DistanceFunction, options: Map[String, String], k: Int): DataFrame = {
    log.debug("scanning LSH index " + indexname)

    val numOfQueries = options.getOrElse("numOfQ", "3").toInt

    val signatureGenerator = ac.sc.broadcast( new LSHSignatureGenerator(metadata.hashTables, metadata.m))

    import MovableFeature.conv_feature2MovableFeature
    val originalQuery = signatureGenerator.value.toBuckets(q)
    //move the query around by the precomuted radius
    //TODO: possibly adjust weight of computed queries vs. true query
    val queries = ac.sc.broadcast(List.fill(numOfQueries)((1.0, signatureGenerator.value.toBuckets(q.move(metadata.radius)))) ::: List((1.0, originalQuery)))

    import org.apache.spark.sql.functions.udf
    val distUDF = udf((c: BitString[_]) => {
      var i = 0
      var score = 0
      val cells = signatureGenerator.value.toBuckets(c)

      while (i < queries.value.length) {
        var j = 0
        var sum = 0

        val weight = queries.value(i)._1
        val query = queries.value(i)._2

        while(j < cells.length){
          if(cells(j) == query(j)){
            sum += 1
          }

          j += 1
        }

        score += sum
        i += 1
      }

      score
    })


    data
      .withColumn(FieldNames.distanceColumnName, distUDF(data(FieldNames.featureIndexColumnName)))
      .filter(col(FieldNames.distanceColumnName) > 0)
  }

  override def isQueryConform(nnq: NearestNeighbourQuery): Boolean = {
    nnq.distance.getClass == metadata.distance.getClass
  }
}

object LSHIndex {
  def apply(indexname: IndexName, entityname: EntityName, data: DataFrame, meta: Any)(implicit ac: AdamContext): LSHIndex = {
    val indexMetaData = meta.asInstanceOf[LSHIndexMetaData]
    new LSHIndex(indexname, entityname, data, indexMetaData)
  }
}