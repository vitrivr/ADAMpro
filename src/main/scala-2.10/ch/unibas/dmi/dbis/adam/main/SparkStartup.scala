package ch.unibas.dmi.dbis.adam.main

import ch.unibas.dmi.dbis.adam.datatypes.bitString.{BitString, MinimalBitString}
import ch.unibas.dmi.dbis.adam.datatypes.feature.FeatureVectorWrapper
import ch.unibas.dmi.dbis.adam.storage.components.{FeatureStorage, IndexStorage, MetadataStorage}
import ch.unibas.dmi.dbis.adam.storage.engine.{LevelDBDataStorage, ParquetDataStorage, PostgresDataStorage}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
object SparkStartup {
  val sparkConfig = new SparkConf().setAppName("ADAMtwo")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryoserializer.buffer.max", "512m")
    .set("spark.kryoserializer.buffer", "256")
    .set("spark.cassandra.connection.host", "192.168.99.100")
    .set("spark.cassandra.auth.username", "cassandra")
    .set("spark.cassandra.auth.password", "cassandra")
    .registerKryoClasses(Array(classOf[BitString[_]], classOf[MinimalBitString], classOf[FeatureVectorWrapper]))

  val sc = new SparkContext(sparkConfig)
  val sqlContext = new HiveContext(sc)

  val featureStorage : FeatureStorage = LevelDBDataStorage
  val metadataStorage : MetadataStorage = PostgresDataStorage
  val indexStorage: IndexStorage = ParquetDataStorage
}