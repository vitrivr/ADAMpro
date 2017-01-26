package org.vitrivr.adampro.main

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.vitrivr.adampro.catalog.{CatalogOperator, LogOperator}
import org.vitrivr.adampro.config.AdamConfig
import org.vitrivr.adampro.helpers.optimizer.OptimizerRegistry
import org.vitrivr.adampro.utils.Logging

/**
  * adamtwo
  *
  * Ivan Giangreco
  * August 2015
  */
object SparkStartup extends Logging {
  object Implicits extends AdamContext {
    implicit lazy val ac = this

    private lazy val conf = {
      val conf = new SparkConf()
        .setAppName("ADAMpro")
        .set("spark.driver.maxResultSize", "1g")
        .set("spark.executor.memory", "2g")
        .set("spark.rpc.message.maxSize", "1024")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryoserializer.buffer.max", "2047m")
        .set("spark.kryoserializer.buffer", "2047")

      if (config.master.isDefined) {
        conf.setMaster(config.master.get)
      }

      conf
    }

    @transient implicit lazy val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    @transient implicit lazy val sc = spark.sparkContext

    @transient implicit lazy val config = new AdamConfig()
    sc.setLogLevel(config.loglevel)

    @transient implicit lazy val sqlContext = spark.sqlContext
  }

  val mainContext = Implicits.ac
  val contexts = Seq(mainContext)

  val logOperator = new LogOperator(mainContext.config.internalsPath)
  val catalogOperator = new CatalogOperator(mainContext.config.internalsPath)

  mainContext.config.engines.zipWithIndex.foreach { case(engine, priority) => mainContext.storageHandlerRegistry.value.register(engine, mainContext.config.engines.length - priority)(mainContext) }
  OptimizerRegistry.loadDefault()(mainContext)
}