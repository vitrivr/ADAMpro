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
object SparkStartup {
    System.setProperty("spark.serializer", "spark.KryoSerializer"); // kryo is much faster
    System.setProperty("spark.kryoserializer.buffer.mb", "1024"); // I serialize bigger objects
    System.setProperty("spark.mesos.coarse", "true"); // link provided
    System.setProperty("spark.akka.frameSize", "500"); // workers should be able to send bigger messages
    System.setProperty("spark.akka.askTimeout", "30"); // high CPU/IO load

    val sparkConfig = new SparkConf().setAppName("ADAMtwo").setMaster("local[16]")
    sparkConfig.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConfig.registerKryoClasses(Array())

    val sc = new SparkContext(sparkConfig)
    //val sqlContext = new SQLContext(sc)
    val sqlContext = new HiveContext(sc)

    val tableStorage: TableStorage = ParquetDataStorage
    val indexStorage: IndexStorage = ParquetDataStorage
}
