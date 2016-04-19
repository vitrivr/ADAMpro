package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.datatypes.feature.{FeatureVectorWrapper, FeatureVectorWrapperUDT}
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.main.SparkStartup.Implicits._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, StructType}

import scala.util.Random

/**
  * adampro
  *
  * Ivan Giangreco
  * March 2016
  */
object RandomDataOp {
  def apply(entityname: EntityName, collectionSize: Int, vectorSize: Int): Boolean = {
    val limit = 10000

    if (CreateEntityOp(entityname).isFailure) {
      return false
    }

    val schema = StructType(Seq(
      StructField(FieldNames.featureColumnName, new FeatureVectorWrapperUDT, false)
    ))

    import SparkStartup.Implicits._
    (0 until collectionSize).sliding(limit, limit).foreach { seq =>
      val rdd = ac.sc.parallelize(seq.map(idx => Row(new FeatureVectorWrapper(Seq.fill(vectorSize)(Random.nextFloat())))))
      val data = sqlContext.createDataFrame(rdd, schema)
      InsertOp(entityname, data)
    }

    true
  }
}