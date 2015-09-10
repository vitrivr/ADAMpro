package ch.unibas.dmi.dbis.adam.main

import ch.unibas.dmi.dbis.adam.storage.components.{IndexStorage, TableStorage}
import ch.unibas.dmi.dbis.adam.storage.engine.ParquetDataStorage
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
object  SparkStartup {
    System.setProperty("spark.serializer", "spark.KryoSerializer");
    System.setProperty("spark.kryoserializer.buffer.max", "256m");
    System.setProperty("spark.kryoserializer.buffer", "1m");
    System.setProperty("spark.mesos.coarse", "true");
    System.setProperty("spark.akka.frameSize", "500");
    System.setProperty("spark.akka.askTimeout", "30");

    val sparkConfig = new SparkConf().setAppName("ADAMtwo").setMaster("local[64]")
    sparkConfig.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConfig.registerKryoClasses(Array()) //TODO: check this!

    val sc = new SparkContext(sparkConfig)
    //val sqlContext = new SQLContext(sc)
    val sqlContext = new HiveContext(sc)

    val tableStorage: TableStorage = ParquetDataStorage
    val indexStorage: IndexStorage = ParquetDataStorage
}
