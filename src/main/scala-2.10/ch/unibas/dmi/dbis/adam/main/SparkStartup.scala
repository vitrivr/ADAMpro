package ch.unibas.dmi.dbis.adam.main

import ch.unibas.dmi.dbis.adam.config.AdamConfig
import ch.unibas.dmi.dbis.adam.storage.components.{FeatureStorage, IndexStorage, MetadataStorage}
import ch.unibas.dmi.dbis.adam.storage.engine.{ParquetFeatureStorage, ParquetIndexStorage, PostgresqlMetadataStorage}
import org.apache.log4j.Logger
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * adamtwo
  *
  * Ivan Giangreco
  * August 2015
  */
object SparkStartup {
  val log = Logger.getLogger(getClass.getName)

  log.debug("Spark starting up")

  val sparkConfig = new SparkConf().setAppName("ADAMpro")
    .set("spark.driver.maxResultSize", "1000m")
    .set("spark.kryoserializer.buffer.max", "2047m")
    .set("spark.kryoserializer.buffer", "2047")
    .set("spark.akka.frameSize", "1024")
    .set("spark.scheduler.mode", "FAIR")
  //TODO: add kryo serializer back, but check with deployment to yarn
  //.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  //.registerKryoClasses(Array(classOf[BitString[_]], classOf[MinimalBitString], classOf[FeatureVectorWrapper]))

  if (AdamConfig.master.isDefined) {
    sparkConfig.setMaster(AdamConfig.master.get)
  }

  object Implicits extends AdamContext {
    implicit lazy val ac = this

    @transient implicit lazy val sc = new SparkContext(sparkConfig)
    sc.setLogLevel(AdamConfig.loglevel)
    //TODO: possibly switch to a jobserver (https://github.com/spark-jobserver/spark-jobserver), pass sqlcontext around
    @transient implicit lazy val sqlContext = new HiveContext(sc)
  }

  val featureStorage: FeatureStorage = ParquetFeatureStorage
  val metadataStorage: MetadataStorage = PostgresqlMetadataStorage
  val indexStorage: IndexStorage = ParquetIndexStorage
}