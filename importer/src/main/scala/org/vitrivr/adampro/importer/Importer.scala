package org.vitrivr.adampro.importer

import java.nio.file.{Files, Paths}

import com.google.protobuf.CodedInputStream
import org.apache.log4j.Logger
import org.vitrivr.adampro.grpc.grpc.InsertMessage.TupleInsertMessage
import org.vitrivr.adampro.grpc.grpc._
import org.vitrivr.adampro.rpc.RPCClient

import scala.collection.mutable.ListBuffer


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

      if (args.length < 1) {
        log.error("Usage: Importer path")
        sys.exit(1)
      }

      importProtoFiles(args(0), grpc)
    } catch {
      case e: Exception => log.error("error while importing", e)
    }
  }

  /**
    *
    * @param path
    * @param grpc
    */
  def importProtoFiles(path: String, grpc: RPCClient) = {
    if (!Paths.get(path).toFile.exists()) {
      throw new Exception("Path does not exist.")
    }

    val paths = files(path)

    log.trace("counting number of files in path")

    val length = files(path).length
    var remaining = length

    log.info("will process " + remaining + " files")

    paths.grouped(10).foreach(groupedPaths => {
      val batch = new ListBuffer[InsertMessage]()

      log.trace("starting new batch")

      groupedPaths.foreach { gpath =>
        try {
          val entity = gpath.getFileName.toString.replace(".bin", "")
          val in = CodedInputStream.newInstance(Files.newInputStream(gpath))

          while (!in.isAtEnd) {
            val tuple = TupleInsertMessage.parseDelimitedFrom(in).get

            val msg = InsertMessage(entity, Seq(tuple))
            batch += msg
            println(entity)
          }

          this.synchronized {
            remaining = remaining - 1
          }
        } catch {
          case e: Exception => log.error("exception while reading files", e)
        }
      }

      log.trace("inserting batch of length " + batch.length)

      val res = grpc.entityBatchInsert(batch)
      if (res.isFailure) {
        log.error("exception while inserting files", res.failed.get)
      }

      log.debug("status: " + remaining + "/" + length)
    })
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