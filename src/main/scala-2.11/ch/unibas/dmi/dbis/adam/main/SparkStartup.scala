package ch.unibas.dmi.dbis.adam.main

import ch.unibas.dmi.dbis.adam.storage.engine.ParquetDataStorage
import ch.unibas.dmi.dbis.adam.storage.components.{IndexStorage, TableStorage}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
object SparkStartup {
  val sparkConfig = new SparkConf().setAppName("ADAMtwo").setMaster("local[2]")
  sparkConfig.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  sparkConfig.registerKryoClasses(Array())

  val sc = new SparkContext(sparkConfig)
  val sqlContext = new SQLContext(sc)

  val tableStorage : TableStorage = ParquetDataStorage
  val indexStorage : IndexStorage = ParquetDataStorage
}
