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
    */
  def apply(path: String, insertOp : (InsertMessage) => (Future[AckMessage]), observer : StreamObserver[AckMessage]) {
    if (!Paths.get(path).toFile.exists()) {
      throw new GeneralAdamException("path does not exist")
    }

    val paths = files(path)

    log.trace("counting number of files in path")

    val length = files(path).length
    var remaining = length

    log.info("will process " + remaining + " files")

    paths.grouped(BATCH_SIZE).foreach(groupedPaths => {
      val batch = new ListBuffer[InsertMessage]()
      log.trace("starting new batch")

      val it = groupedPaths.iterator

      while (it.hasNext) {
        val gpath = it.next

        try {
          val entity = gpath.replace(".bin", "")

          val is = new FileInputStream(gpath)

          try {
            val in = CodedInputStream.newInstance(is)

            while (!in.isAtEnd) {
              val tuple = TupleInsertMessage.parseDelimitedFrom(in).get

              val msg = InsertMessage(entity, Seq(tuple))

              batch += msg
            }
          } catch {
            case e: Exception => log.error("exception while reading files: " + gpath, e)
          } finally {
            is.close()
          }

          this.synchronized {
            remaining = remaining - 1
          }
        } catch {
          case e: Exception => log.error("exception while reading files: " + gpath, e)
        }
      }

      log.trace("inserting batch of length " + batch.length)

      val inserts = batch.groupBy(_.entity).mapValues(_.flatMap(_.tuples)).map { case (entity, tuples) => InsertMessage(entity, tuples) }.toSeq

      inserts.foreach { insert =>
        val res = Await.result(insertOp(insert), Duration.Inf)
        if (res.code != AckMessage.Code.OK) {
          log.error("exception while inserting files: " + groupedPaths.mkString(";"), res.message)
        }
        observer.onNext(AckMessage(res.code, groupedPaths.mkString(";")))
      }

      log.info("status: " + remaining + "/" + length)
    })

    observer.onCompleted()
  }

  /**
    *
    * @param path
    * @return
    */
  private def files(path: String) = {
    import scala.collection.JavaConversions._
    FileUtils.listFiles(new File(path), Array(".bin"), true).map(_.getAbsolutePath).toSeq.sortBy(_.reverse).iterator
  }
}
