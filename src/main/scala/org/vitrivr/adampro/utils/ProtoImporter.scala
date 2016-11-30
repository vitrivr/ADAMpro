package org.vitrivr.adampro.utils

import java.io._
import java.nio.file.Paths

import com.google.protobuf.CodedInputStream
import io.grpc.stub.StreamObserver
import org.apache.commons.io.FileUtils
import org.vitrivr.adampro.exception.GeneralAdamException
import org.vitrivr.adampro.grpc.grpc.InsertMessage.TupleInsertMessage
import org.vitrivr.adampro.grpc.grpc.{AckMessage, InsertMessage}

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * November 2016
  */
object ProtoImporter extends Serializable with Logging {
  private val BATCH_SIZE = 1000

  /**
    *
    * @param path
    * @param insertOp
    * @param observer
    */
  def apply(path: String, insertOp: (InsertMessage) => (Future[AckMessage]), observer: StreamObserver[AckMessage]) {
    if (!Paths.get(path).toFile.exists()) {
      throw new GeneralAdamException("path does not exist")
    }

    lazy val paths = {
      import scala.collection.JavaConverters._
      FileUtils.listFiles(new File(path), Array("bin"), true).asScala.toList.sortBy(_.getAbsolutePath.reverse)
    }

    log.trace("counting number of files in path")

    val length = paths.length
    var done = 0

    log.info("will process " + length + " files")

    paths.grouped(BATCH_SIZE).foreach( pathBatch => {
      val batch = new ListBuffer[InsertMessage]()
      log.trace("starting new batch")

      pathBatch.foreach { path =>
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

      log.trace("inserting batch of length " + batch.length)

      val inserts = batch.groupBy(_.entity).mapValues(_.flatMap(_.tuples)).map { case (entity, tuples) => InsertMessage(entity, tuples) }.toSeq

      inserts.foreach { insert =>
        val res = Await.result(insertOp(insert), Duration.Inf)
        if (res.code != AckMessage.Code.OK) {
          log.error("exception while inserting files: " + pathBatch.mkString(";"), res.message)
        }
        observer.onNext(AckMessage(res.code, pathBatch.mkString(";")))
      }

      log.info("status: " + done + "/" + length)
    })

    observer.onCompleted()
  }
}
