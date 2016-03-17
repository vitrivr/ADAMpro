package ch.unibas.dmi.dbis.adam.test

import java.sql.DriverManager

import ch.unibas.dmi.dbis.adam.config.{AdamConfig, FieldNames}
import ch.unibas.dmi.dbis.adam.datatypes.feature.{FeatureVectorWrapper, FeatureVectorWrapperUDT}
import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Row, types}
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.{FeatureSpec, GivenWhenThen}

import scala.util.Random

/**
  * adampro
  *
  * Ivan Giangreco
  * March 2016
  */
class AdamBaseTest extends FeatureSpec with GivenWhenThen with Eventually with IntegrationPatience {
  SparkStartup

  /**
    *
    * @return
    */
  def connection = {
    Class.forName("org.postgresql.Driver").newInstance
    DriverManager.getConnection(AdamConfig.jdbcUrl, AdamConfig.jdbcUser, AdamConfig.jdbcPassword)
  }


  /**
    *
    * @param len
    * @return
    */
  def getRandomName(len: Int = 10) = {
    val sb = new StringBuilder(len)
    val ab = "abcdefghijklmnopqrstuvwxyz"
    for (i <- 0 until len) {
      sb.append(ab(Random.nextInt(ab.length)))
    }
    sb.toString
  }


  /**
    *
    * @param ntuples
    * @param ndims
    * @return
    */
  def createSimpleEntity(ntuples: Int, ndims: Int): String = {
    val entityname = getRandomName()
    Entity.create(entityname)

    val schema = StructType(Seq(
      StructField(FieldNames.featureColumnName, new FeatureVectorWrapperUDT, false)
    ))

    val rdd = SparkStartup.sc.parallelize((0 until ntuples).map(id =>
      Row(new FeatureVectorWrapper(Seq.fill(ndims)(Random.nextFloat())))
    ))

    val data = SparkStartup.sqlContext.createDataFrame(rdd, schema)

    Entity.insertData(entityname, data)

    entityname
  }


  /**
    *
    * @param ntuples
    * @param ndims
    * @return
    */
  def createEntityWithMetadata(ntuples: Int, ndims: Int): String = {
    val entityname = getRandomName()
    Entity.create(entityname)

    val stringLength = 10
    val maxInt = 50000

    When("data is inserted")
    val schema = StructType(Seq(
      StructField(FieldNames.featureColumnName, new FeatureVectorWrapperUDT, false),
      StructField("stringfield", types.StringType, false),
      StructField("floatfield", types.FloatType, false),
      StructField("doublefield", types.DoubleType, false),
      StructField("intfield", types.IntegerType, false),
      StructField("longfield", types.LongType, false),
      StructField("booleanfield", types.BooleanType, false)
    ))

    val rdd = SparkStartup.sc.parallelize((0 until ntuples).map(id =>
      Row(getRandomFeatureVector(ndims),
        Random.nextString(stringLength),
        math.abs(Random.nextFloat()),
        math.abs(Random.nextDouble()),
        math.abs(Random.nextInt(maxInt)),
        math.abs(Random.nextLong()),
        Random.nextBoolean()
      )))

    val data = SparkStartup.sqlContext.createDataFrame(rdd, schema)

    Entity.insertData(entityname, data)

    entityname
  }

  /**
    *
    * @param ndims
    * @return
    */
  def getRandomFeatureVector(ndims : Int) = new FeatureVectorWrapper(Seq.fill(ndims)(Random.nextFloat()))
}
