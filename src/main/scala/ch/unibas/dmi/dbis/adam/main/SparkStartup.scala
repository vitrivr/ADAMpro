package ch.unibas.dmi.dbis.adam.main

import ch.unibas.dmi.dbis.adam.config.AdamConfig
import ch.unibas.dmi.dbis.adam.datatypes.bitString.BitStringUDT
import ch.unibas.dmi.dbis.adam.datatypes.feature.FeatureVectorWrapperUDT
import ch.unibas.dmi.dbis.adam.storage.components.{FeatureStorage, IndexStorage, MetadataStorage}
import ch.unibas.dmi.dbis.adam.storage.engine.{ParquetFeatureStorage, ParquetIndexStorage, PostgresqlMetadataStorage}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
  * adamtwo
  *
  * Ivan Giangreco
  * August 2015
  */
object SparkStartup extends Logging {
  val sparkConfig = new SparkConf().setAppName("ADAMpro")
    .set("spark.driver.maxResultSize", "1000m")
    .set("spark.kryoserializer.buffer.max", "2047m")
    .set("spark.kryoserializer.buffer", "2047")
    .set("spark.akka.frameSize", "1024")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .registerKryoClasses(Array(classOf[BitStringUDT], classOf[FeatureVectorWrapperUDT]))
    .set("spark.scheduler.allocation.file", AdamConfig.schedulerFile)

  if (AdamConfig.master.isDefined) {
    sparkConfig.setMaster(AdamConfig.master.get)
  }

  object Implicits extends AdamContext {
    implicit lazy val ac = this

    @transient implicit lazy val sc = new SparkContext(sparkConfig)
    sc.hadoopConfiguration.set("parquet.enable.summary-metadata", "false")
    sc.setLogLevel(AdamConfig.loglevel)
    //TODO: possibly switch to a jobserver (https://github.com/spark-jobserver/spark-jobserver), pass sqlcontext around

    //TODO: possibly adjust block and page size
    // val blockSize = 1024 * 1024 * 16      // 16MB
    // sc.hadoopConfiguration.setInt( "dfs.blocksize", blockSize )
    // sc.hadoopConfiguration.setInt( "parquet.block.size", blockSize )
    //also consider: https://issues.apache.org/jira/browse/SPARK-7263


    @transient implicit lazy val sqlContext = new HiveContext(sc)
  }

  val featureStorage: FeatureStorage = ParquetFeatureStorage
  val metadataStorage: MetadataStorage = PostgresqlMetadataStorage
  val indexStorage: IndexStorage = ParquetIndexStorage
}