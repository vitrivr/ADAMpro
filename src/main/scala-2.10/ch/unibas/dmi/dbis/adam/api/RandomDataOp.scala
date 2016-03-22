package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.datatypes.feature.{FeatureVectorWrapperUDT, FeatureVectorWrapper}
import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType, StructField}

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

    val schema = StructType(Seq(
      StructField(FieldNames.featureColumnName, new FeatureVectorWrapperUDT, false)
    ))

    val rdd = SparkStartup.sc.parallelize((0 until math.max(1, collectionSize / limit)).map(id =>
      Row(new FeatureVectorWrapper(Seq.fill(vectorSize)(Random.nextFloat())))
    ))

    val data = SparkStartup.sqlContext.createDataFrame(rdd, schema)

    Entity.insertData(entityname, data)

    true
  }
}