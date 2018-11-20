package org.vitrivr.adampro.process

import java.net.{InetAddress, NetworkInterface}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.vitrivr.adampro.config.AdamConfig
import org.vitrivr.adampro.query.planner.PlannerRegistry
import org.vitrivr.adampro.utils.Logging

/**
  * adamtwo
  *
  * Ivan Giangreco
  * August 2015
  */
object SparkStartup extends Logging {
  object Implicits extends SharedComponentContext {
    implicit lazy val scc = this

    private lazy val conf = {
      val conf = new SparkConf()
        .setAppName("ADAMpro")
        .set("spark.driver.maxResultSize", "0")
        .set("spark.rpc.message.maxSize", "1024")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryoserializer.buffer.max", "2047m")
        .set("spark.kryoserializer.buffer", "2047")
        .set("spark.network.timeout", "240s")
        .set("spark.sql.autoBroadcastJoinThreshold", (50 * 1024 * 1024).toString)
        .set("spark.sql.files.openCostInBytes", (256 * 1024 * 1024).toString) // see [SPARK-19629]


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
    //the following line has been added to to a bug related to SPARK-18883 and SPARK-15849
    sc.hadoopConfiguration.set("mapreduce.fileoutputcommitter.algorithm.version", "2")

    @transient implicit lazy val sqlContext = spark.sqlContext
  }

  val mainContext = Implicits.scc
  val contexts = Seq(mainContext)


  if(isLocal(InetAddress.getByName(mainContext.sc.getConf.get("spark.driver.host")))){
    //start RPC only on master
    new Thread(new RPCStartup(SparkStartup.mainContext.config.grpcPort)).start
  } else {
    log.warn("not starting RPC connection, as node runs worker")
  }


  /**
    *
    * @param ipaddr
    * @return
    */
  private def isLocal(ipaddr : InetAddress) : Boolean = {
    if (ipaddr.isAnyLocalAddress || ipaddr.isLoopbackAddress){
      true
    } else {
      try {
        (NetworkInterface.getByInetAddress(ipaddr) != null)
      } catch {
        case e : Exception => false
      }
    }
  }
}