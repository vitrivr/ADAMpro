package ch.unibas.dmi.dbis.adam.main

import ch.unibas.dmi.dbis.adam.config.AdamConfig
import ch.unibas.dmi.dbis.adam.datatypes.bitString.BitStringUDT
import ch.unibas.dmi.dbis.adam.datatypes.feature.FeatureVectorWrapperUDT
import ch.unibas.dmi.dbis.adam.storage.engine.instances.{ParquetEngine, PostgresqlEngine}
import ch.unibas.dmi.dbis.adam.storage.handler._
import ch.unibas.dmi.dbis.adam.utils.Logging
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * adamtwo
  *
  * Ivan Giangreco
  * August 2015
  */
object SparkStartup extends Logging {
  val sparkConfig = new SparkConf().setAppName("ADAMpro")
    .set("spark.driver.maxResultSize", "1000m")
    .set("spark.akka.frameSize", "1024")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryoserializer.buffer.max", "2047m")
    .set("spark.kryoserializer.buffer", "2047")
    .registerKryoClasses(Array(classOf[BitStringUDT], classOf[FeatureVectorWrapperUDT]))
    .set("spark.scheduler.allocation.file", AdamConfig.schedulerFile)
    .set("spark.driver.allowMultipleContexts", "true")

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

  val mainContext = Implicits.ac
  val contexts = Seq(mainContext)

  //TODO: add dynamically based on config
  val storageRegistry = StorageHandlerRegistry
  storageRegistry.register(new DatabaseHandler(new PostgresqlEngine(AdamConfig.jdbcUrl, AdamConfig.jdbcUser, AdamConfig.jdbcPassword)))

  if (AdamConfig.isBaseOnHadoop) {
    log.debug("storing data on Hadoop")
    storageRegistry.register(new FlatFileHandler(new ParquetEngine(AdamConfig.basePath, AdamConfig.dataPath)))
  } else {
    log.debug("storing data locally")
    storageRegistry.register(new FlatFileHandler(new ParquetEngine(AdamConfig.dataPath)))
  }

  storageRegistry.register(new SolrHandler(AdamConfig.solrUrl))

  val indexStorageHandler = if (AdamConfig.isBaseOnHadoop) {
    log.debug("storing data on Hadoop")
    new IndexFlatFileHandler(new ParquetEngine(AdamConfig.basePath, AdamConfig.indexPath))
  } else {
    new IndexFlatFileHandler(new ParquetEngine(AdamConfig.indexPath))
  }
}