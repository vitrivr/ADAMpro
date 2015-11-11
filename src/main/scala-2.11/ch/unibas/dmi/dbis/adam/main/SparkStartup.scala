package ch.unibas.dmi.dbis.adam.main

import ch.unibas.dmi.dbis.adam.datatypes.bitString.{BitString, MinimalBitString}
import ch.unibas.dmi.dbis.adam.datatypes.feature.FeatureVectorWrapper
import ch.unibas.dmi.dbis.adam.storage.components.{MetadataStorage, IndexStorage, FeatureStorage}
import ch.unibas.dmi.dbis.adam.storage.engine.{PostgresDataStorage, LevelDBDataStorage, ParquetDataStorage}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
object SparkStartup {
  val sparkConfig = new SparkConf().setAppName("ADAMtwo").setMaster("local[64]")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryoserializer.buffer.max", "512m")
    .set("spark.kryoserializer.buffer", "256")
    .set("spark.driver.maxResultSize", "0")
    .set("spark.driver.memory", "9g")
    .set("spark.driver.host", "localhost")
    .set("spark.rdd.compress", "true")
    .set("spark.parquet.block.size", (1024 * 1024 * 16).toString)
    .set("spark.sql.parquet.compression.codec", "gzip")
    .set("spark.cassandra.connection.host", "192.168.123.10")
    .set("spark.cassandra.auth.username", "cassandra")
    .set("spark.cassandra.auth.password", "cassandra")
    .set("spark.scheduler.allocation.file", "/Users/gianiv01/Development/adamtwo/conf/scheduler.xml")
    .registerKryoClasses(Array(classOf[BitString[_]], classOf[MinimalBitString], classOf[FeatureVectorWrapper]))

  val sc = new SparkContext(sparkConfig)
  //val sqlContext = new SQLContext(sc)
  val sqlContext = new HiveContext(sc)
  sqlContext.setConf("hive.metastore.warehouse.dir", Startup.config.hivePath.toAbsolute.toString())


  sqlContext.setConf("spark.sql.avro.compression.codec", "deflate")
  sqlContext.setConf("spark.sql.avro.deflate.level", "5")
  sqlContext.setConf("spark.parquet.block.size", (1024 * 1024 * 8).toString)

  val featureStorage : FeatureStorage = LevelDBDataStorage
  val metadataStorage : MetadataStorage = PostgresDataStorage
  val indexStorage: IndexStorage = ParquetDataStorage
}