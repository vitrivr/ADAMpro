package org.vitrivr.adampro.utils

import java.io._

import com.google.protobuf.CodedInputStream
import io.grpc.stub.StreamObserver
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.vitrivr.adampro.datatypes.feature.{FeatureVectorWrapper, FeatureVectorWrapperUDT}
import org.vitrivr.adampro.exception.GeneralAdamException
import org.vitrivr.adampro.grpc.grpc.InsertMessage.TupleInsertMessage
import org.vitrivr.adampro.grpc.grpc._

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * November 2016
  */
object ProtoImporterExporter extends Serializable with Logging {
  private val BATCH_SIZE = 1000

  //TODO: distinguish .bin file with data and .catalog file with entity information

  /**
    *
    * @param path
    * @param insertOp
    * @param observer
    */
  def importData(path: String, insertOp: (InsertMessage) => (Future[AckMessage]), observer: StreamObserver[AckMessage]) {
    if (!new File(path).exists()) {
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

    paths.grouped(BATCH_SIZE).foreach(pathBatch => {
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

  /**
    *
    * @param path
    * @param entityname
    * @param data
    */
  def exportData(path: String, entityname: String, data: DataFrame): Try[Void] = {
    val file = if (new File(path).exists() && new File(path).isDirectory) {
      new File(path, entityname + ".bin")
    } else {
      throw new GeneralAdamException("please specify the path to an existing folder")
    }

    try {
      //data
      val cols = data.schema

      val messages = data.map(row => {
        val metadata = cols.map(col => {
          try {
            col.name -> {
              col.dataType match {
                case BooleanType => DataMessage().withBooleanData(row.getAs[Boolean](col.name))
                case DoubleType => DataMessage().withDoubleData(row.getAs[Double](col.name))
                case FloatType => DataMessage().withFloatData(row.getAs[Float](col.name))
                case IntegerType => DataMessage().withIntData(row.getAs[Integer](col.name))
                case LongType => DataMessage().withLongData(row.getAs[Long](col.name))
                case StringType => DataMessage().withStringData(row.getAs[String](col.name))
                case _: FeatureVectorWrapperUDT => DataMessage().withFeatureData(FeatureVectorMessage().withDenseVector(DenseVectorMessage(row.getAs[FeatureVectorWrapper](col.name).toSeq)))
                case _ => DataMessage().withStringData("")
              }
            }
          } catch {
            case e: Exception => col.name -> DataMessage().withStringData("")
          }
        }).toMap

        TupleInsertMessage(metadata)
      })

      val fos = new FileOutputStream(file)

      messages.toLocalIterator.foreach { m =>
        m.writeDelimitedTo(fos)
        fos.flush()
      }

      fos.close()

      //catalog data is currently not exported
      /*val attributes = entity.schema().map(attribute => {
          AttributeDefinitionMessage(attribute.name, matchFields(attribute.fieldtype), attribute.pk, attribute.params, attribute.storagehandler.name)
        })

        val createEntity = new CreateEntityMessage(entityname, attributes)*/

      Success(null)
    } catch {
      case e: Exception => Failure(e)
    }
  }
}
