package org.vitrivr.adampro.importer

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.{Files, Paths}

import com.google.protobuf.CodedInputStream
import org.apache.log4j.Logger
import org.vitrivr.adampro.grpc.grpc.InsertMessage.TupleInsertMessage
import org.vitrivr.adampro.grpc.grpc._
import org.vitrivr.adampro.rpc.RPCClient

import scala.collection.immutable.HashSet
import scala.collection.mutable.ListBuffer
import scala.io.Source


/**
  * ADAMpro
  *
  * Ivan Giangreco
  * November 2016
  */
object Importer {
  val log = Logger.getLogger(getClass.getName)

  def main(args: Array[String]) {
    try {
      val grpcHost = "localhost"
      val grpcPort = 5890

      val grpc = RPCClient(grpcHost, grpcPort)

      if (args.length < 2) {
        log.error("Usage: Importer path logpath")
        sys.exit(1)
      }

      createEntities(grpc)
      importProtoFiles(args(0), args(1), grpc)
    } catch {
      case e: Exception => log.error("error while importing", e)
    }
  }


  /**
    *
    * @param grpc
    */
  def createEntities(grpc: RPCClient): Unit = {
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
    *
    * @param path
    * @param proplogpath
    * @param grpc
    */
  def importProtoFiles(path: String, proplogpath: String, grpc: RPCClient) = {
    if (!Paths.get(path).toFile.exists()) {
      throw new Exception("Path does not exist.")
    }

    var logpath = proplogpath
    var filter = HashSet[String]()

    if (Paths.get(logpath).toFile.exists()) {
      logpath = proplogpath + System.currentTimeMillis()

      filter = HashSet().++(Source.fromFile(proplogpath).getLines)
    }

    val logfile = new File(logpath)
    val pathLogger = new BufferedWriter(new FileWriter(logfile))

    val paths = files(path)

    log.trace("counting number of files in path")

    val length = files(path).length
    var remaining = length

    log.info("will process " + remaining + " files")

    paths.grouped(10).foreach(groupedPaths => {
      val batch = new ListBuffer[InsertMessage]()
      val tmpPathLogs = new ListBuffer[String]()
      log.trace("starting new batch")

      groupedPaths.foreach { gpath =>
        if (!filter.contains(gpath.toAbsolutePath.toString)) {
          try {
            tmpPathLogs += gpath.toAbsolutePath.toString

            val entity = gpath.getFileName.toString.replace(".bin", "")
            val in = CodedInputStream.newInstance(Files.newInputStream(gpath))

            while (!in.isAtEnd) {
              val tuple = TupleInsertMessage.parseDelimitedFrom(in).get

              val msg = InsertMessage(entity, Seq(tuple))
              batch += msg
            }

            this.synchronized {
              remaining = remaining - 1
            }
          } catch {
            case e: Exception => log.error("exception while reading files: " + gpath, e)
          }
        }
      }

      log.trace("inserting batch of length " + batch.length)

      val res = grpc.entityBatchInsert(batch)
      if (res.isFailure) {
        log.error("exception while inserting files", res.failed.get)
      }

      tmpPathLogs.foreach { tmpPathLog =>
        pathLogger.write(tmpPathLog)
        pathLogger.write("\n")
      }
      pathLogger.flush()

      log.debug("status: " + remaining + "/" + length)
    })

    pathLogger.close()
  }

  /**
    *
    * @param path
    * @return
    */
  def files(path: String) = {
    import scala.collection.JavaConversions._
    Files.walk(Paths.get(path)).iterator().filter(_.toString.endsWith(".bin"))
  }
}