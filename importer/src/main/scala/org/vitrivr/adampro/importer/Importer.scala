package org.vitrivr.adampro.importer

import java.io._
import java.nio.file.Paths

import com.google.protobuf.CodedInputStream
import io.grpc.stub.StreamObserver
import org.apache.commons.io.FileUtils
import org.slf4j.LoggerFactory
import org.vitrivr.adampro.grpc.grpc.InsertMessage.TupleInsertMessage
import org.vitrivr.adampro.grpc.grpc._
import org.vitrivr.adampro.rpc.RPCClient
import org.vitrivr.adampro.rpc.datastructures.RPCAttributeDefinition

import scala.collection.immutable.HashSet
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source


/**
  * ADAMpro
  *
  * Ivan Giangreco
  * November 2016
  */
class Importer(grpc: RPCClient) {
  val log = LoggerFactory.getLogger(this.getClass.getName.stripSuffix("$"))

  val runningCounts = mutable.Map[String, Int]()

  private val BATCH_SIZE = 1000

  /**
    *
    */
  def createEntities(): Unit = {
    //TODO: make this more generic, e.g., by importing entity creation messages

    //metadata tables
    grpc.entityCreate("cineast_multimediaobject",
      Seq(
        RPCAttributeDefinition("id", "string", true),
        RPCAttributeDefinition("type", "int"),
        RPCAttributeDefinition("name", "string"),
        RPCAttributeDefinition("path", "string"),
        RPCAttributeDefinition("width", "int"),
        RPCAttributeDefinition("height", "int"),
        RPCAttributeDefinition("framecount", "int"),
        RPCAttributeDefinition("duration", "float")
      )
    )

    grpc.entityCreate("cineast_segment",
      Seq(
        RPCAttributeDefinition("id", "string", true),
        RPCAttributeDefinition("multimediaobject", "string"),
        RPCAttributeDefinition("sequencenumber", "int"),
        RPCAttributeDefinition("segmentstart", "int"),
        RPCAttributeDefinition("segmentend", "int")
      )
    )

    //feature tables
    val features = Seq(
      "features_AverageColor" -> Seq(("id", "string"), ("feature", "feature")),
      "features_MedianColor" -> Seq(("id", "string"), ("feature", "feature")),
      "features_AverageFuzzyHist" -> Seq(("id", "string"), ("feature", "feature")),
      "features_AverageFuzzyHistNormalized" -> Seq(("id", "string"), ("feature", "feature")),
      "features_MedianFuzzyHist" -> Seq(("id", "string"), ("feature", "feature")),
      "features_AverageColorARP44" -> Seq(("id", "string"), ("feature", "feature")),
      "features_AverageColorARP44Normalized" -> Seq(("id", "string"), ("feature", "feature")),
      "features_SubDivAverageFuzzyColor" -> Seq(("id", "string"), ("feature", "feature")),
      "features_SubDivMedianFuzzyColor" -> Seq(("id", "string"), ("feature", "feature")),
      "features_AverageColorGrid8" -> Seq(("id", "string"), ("feature", "feature")),
      "features_AverageColorGrid8Normalized" -> Seq(("id", "string"), ("feature", "feature")),
      "features_AverageColorCLD" -> Seq(("id", "string"), ("feature", "feature")),
      "features_AverageColorCLDNormalized" -> Seq(("id", "string"), ("feature", "feature")),
      "features_CLD" -> Seq(("id", "string"), ("feature", "feature")),
      "features_CLDNormalized" -> Seq(("id", "string"), ("feature", "feature")),
      "features_MedianColorGrid8" -> Seq(("id", "string"), ("feature", "feature")),
      "features_AverageColorRaster" -> Seq(("id", "string"), ("hist", "feature"), ("rater", "feature")),
      "features_EdgeARP88" -> Seq(("id", "string"), ("feature", "feature")),
      "features_EdgeGrid16" -> Seq(("id", "string"), ("feature", "feature")),
      "features_EHD" -> Seq(("id", "string"), ("feature", "feature")),
      "features_DominantEdgeGrid16" -> Seq(("id", "string"), ("feature", "feature")),
      "features_DominantEdgeGrid8" -> Seq(("id", "string"), ("feature", "feature")),
      "features_SubDivMotionHistogram3" -> Seq(("id", "string"), ("feature", "feature")),
      "features_SubDivMotionHistogram5" -> Seq(("id", "string"), ("feature", "feature")),
      "features_SubDivMotionHistogramBackground3" -> Seq(("id", "string"), ("feature", "feature")),
      "features_SubDivMotionHistogramBackground5" -> Seq(("id", "string"), ("feature", "feature")),
      "features_AverageColorGrid8Reduced11" -> Seq(("id", "string"), ("feature", "feature")),
      "features_AverageColorGrid8Reduced15" -> Seq(("id", "string"), ("feature", "feature")),
      "features_AverageColorRasterReduced11" -> Seq(("id", "string"), ("hist", "feature"), ("rater", "feature")),
      "features_AverageColorRasterReduced15" -> Seq(("id", "string"), ("hist", "feature"), ("rater", "feature")),
      "features_CLDReduced11" -> Seq(("id", "string"), ("feature", "feature")),
      "features_CLDReduced15" -> Seq(("id", "string"), ("feature", "feature"))
    )


    features.foreach { case (entityname, attributes) =>
      grpc.entityCreate(entityname,
        attributes.map { case (name, datatype) => RPCAttributeDefinition(name, datatype, name == "id") }
      )
    }
  }

  /**
    * Serving local files to import. Note that due to some errors when using grpc, having too large batches will fail the import.
    *
    * @param path
    * @param proplogpath
    */
  def serveLocalProtoFiles(path: String, proplogpath: String) = {
    if (!new File(path).exists()) {
      throw new Exception("Path does not exist.")
    }

    val paths = {
      import scala.collection.JavaConverters._
      FileUtils.listFiles(new File(path), Array("bin"), true).asScala.toList.sortBy(_.getAbsolutePath.reverse)
    }

    var logpath = proplogpath
    var filter = HashSet[String]()

    if (Paths.get(logpath).toFile.exists()) {
      logpath = proplogpath + System.currentTimeMillis()

      //import all logs
      filter = HashSet() ++
        Paths.get(proplogpath).getParent.toFile.listFiles(new FilenameFilter {
          override def accept(dir: File, name: String): Boolean = name.startsWith(Paths.get(proplogpath).getFileName.toString)
        }).flatMap(Source.fromFile(_).getLines)
    }

    val logfile = new File(logpath)
    val pathLogger = new BufferedWriter(new FileWriter(logfile))

    log.trace("preparing file list in path")

    val length = paths.length
    var done = 0

    log.info("will process " + length + " files")

    paths.grouped(BATCH_SIZE).foreach(pathBatch => {
      val batch = new ListBuffer[InsertMessage]()
      val tmpPathLogs = new ListBuffer[String]()

      log.trace("starting new batch of length " + pathBatch.length)

      pathBatch.foreach { path =>
        if (!filter.contains(path.getAbsolutePath)) {
          try {
            val entity = path.getName.replace(".bin", "")

            val is = new FileInputStream(path)

            try {
              val in = CodedInputStream.newInstance(is)

              while (!in.isAtEnd) {
                val tuple = TupleInsertMessage.parseDelimitedFrom(in).get

                val msg = InsertMessage(entity, Seq(tuple))

                batch += msg
              }
            } catch {
              case e: Exception => log.error("exception while reading files: " + path, e)
            }

            is.close()

            this.synchronized {
              done += 1
            }
          } catch {
            case e: Exception => log.error("exception while reading files: " + path, e)
          }
        }
      }

      log.trace("inserting batch of length " + batch.length)

      val inserts = batch.groupBy(_.entity).mapValues(_.flatMap(_.tuples)).map { case (entity, tuples) => InsertMessage(entity, tuples) }.toSeq

      inserts.foreach { insert =>
        val res = grpc.entityInsert(insert)
        if (res.isFailure) {
          log.error("exception while inserting files: " + pathBatch.mkString(";"), res.failed.get)
        }
      }

      tmpPathLogs.foreach { tmpPathLog =>
        pathLogger.write(tmpPathLog)
        pathLogger.write("\n")
      }
      pathLogger.flush()

      log.info("status: " + done + "/" + length)
    })

    pathLogger.close()
  }

  /**
    *
    * @param path
    */
  def remoteImportProtoFiles(path: String) = {
    val so = new StreamObserver[(Boolean, String)]() {
      var finished = false

      override def onError(throwable: Throwable): Unit = {
        log.error("error while processing import", throwable)
        finished = true
      }

      override def onCompleted(): Unit = {
        log.info("completed")
        finished = true
      }

      override def onNext(ack: (Boolean, String)): Unit = {
        if (!ack._1) {
          log.error("error while inserting: " + ack._2)
        }
      }
    }

    val res = grpc.entityProtoImport(path, so)

    while (!so.finished) {
      Thread.sleep(1000)
    }
  }


  /**
    *
    * @return
    */
  private def getEntities(): Seq[String] = {
    val entities = grpc.entityList()

    if (entities.isSuccess) {
      entities.get
    } else {
      log.error("could not retrieve list of all entities", entities.failed.get)
      Seq()
    }
  }

  /**
    *
    */
  def vacuumEntities(): Unit = {
    val entities = getEntities()

    val it = entities.iterator
    while (it.hasNext) {
      val entity = it.next()
      try {
        val res = grpc.entityVacuum(entity)

        if (res.isSuccess) {
          log.trace("vacuumed entity " + entity)
        } else {
          throw res.failed.get
        }
      } catch {
        case e: Exception => log.error("could not vacuum entity " + entity, e)
      }
    }
  }


  /**
    *
    */
  def outputCounts(): Unit = {
    getCounts().foreach { case (entity, count) =>
      println(entity + " -> " + count + " (logged: " + runningCounts.getOrElse(entity, "<N/A>") + ")")
    }
  }

  /**
    *
    */
  private def getCounts(): Map[String, String] = {
    getEntities().map(entity => entity -> grpc.entityDetails(entity).map(_.get("count").getOrElse("0")).getOrElse("0")).toMap
  }


}


object Importer {
  val log = LoggerFactory.getLogger(this.getClass.getName.stripSuffix("$"))

  def main(args: Array[String]) {
    try {
      val grpcHost = "localhost"
      val grpcPort = 5890

      val grpc = RPCClient(grpcHost, grpcPort)

      if (args.length < 2) {
        System.err.println("Usage: Importer path logpath")
        sys.exit(1)
      }

      val importer = new Importer(grpc)

      importer.createEntities()
      importer.importProtoFiles(args(0), args(1))
      importer.vacuumEntities()
      importer.outputCounts()
    } catch {
      case e: Exception => log.error("error while importing", e)
    }
  }
}