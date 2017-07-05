package org.vitrivr.adampro.utils.importer

import java.io._

import com.google.protobuf.CodedInputStream
import io.grpc.stub.StreamObserver
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.DataFrame
import org.vitrivr.adampro.data.datatypes.AttributeTypes
import org.vitrivr.adampro.data.datatypes.AttributeTypes.AttributeType
import org.vitrivr.adampro.data.entity.Entity.EntityName
import org.vitrivr.adampro.data.entity.{AttributeDefinition, Entity}
import org.vitrivr.adampro.utils.exception.GeneralAdamException
import org.vitrivr.adampro.grpc.grpc
import org.vitrivr.adampro.grpc.grpc.InsertMessage.TupleInsertMessage
import org.vitrivr.adampro.grpc.grpc._
import org.vitrivr.adampro.process.SharedComponentContext
import org.vitrivr.adampro.utils.Logging

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
class ProtoImporterExporter()(@transient implicit val ac: SharedComponentContext) extends Serializable with Logging {
  private val BATCH_SIZE = 10000

  /**
    *
    * @param path
    * @param createOp
    * @param insertOp
    * @param observer
    */
  def importData(path: String, createOp: (CreateEntityMessage) => (Future[AckMessage]), insertOp: (InsertMessage) => (Future[AckMessage]), observer: StreamObserver[AckMessage]) {
    if (!new File(path).exists()) {
      throw new GeneralAdamException("path does not exist")
    }

    readCatalogFile(getAllFiles(path).filter(_.getName.endsWith("catalog")), createOp, observer)
    readDataFile(getAllFiles(path).filter(_.getName.endsWith("bin")), insertOp, observer)
  }

  /**
    *
    * @param path
    * @return
    */
  private def getAllFiles(path: String) = {
    import scala.collection.JavaConverters._
    FileUtils.listFiles(new File(path), Array("bin", "catalog"), true).asScala.toList.sortBy(_.getAbsolutePath.reverse)
  }

  /**
    *
    * @param files
    * @param op
    * @param observer
    */
  private def readCatalogFile(files: Seq[File], op: (CreateEntityMessage) => (Future[AckMessage]), observer: StreamObserver[AckMessage]): Unit = {
    val catalogfiles = files
    assert(catalogfiles.forall(_.getName.endsWith("catalog")))

    val batch = new ListBuffer[CreateEntityMessage]()

    log.trace("read catalog data")
    catalogfiles.foreach { path =>
      try {
        val is = new FileInputStream(path)

        try {
          val in = CodedInputStream.newInstance(is)

          while (!in.isAtEnd) {
            batch += CreateEntityMessage.parseDelimitedFrom(in).get
          }
        } catch {
          case e: Exception => log.error("exception while reading catalog file: " + path, e)
        }

        is.close()
      } catch {
        case e: Exception => log.error("exception while closing stream to catalog file: " + path, e)
      }
    }

    log.trace("perform creation of entities")
    batch.foreach { message =>
      try {
        op(message)
      } catch {
        case e: Exception => log.error("exception while creating entity " + message.entity, e)
      }
    }
  }

  /**
    *
    * @param files
    * @param op
    * @param observer
    */
  private def readDataFile(files: Seq[File], op: (InsertMessage) => (Future[AckMessage]), observer: StreamObserver[AckMessage]): Unit = {
    val datafiles = files
    assert(datafiles.forall(_.getName.endsWith("bin")))

    val length = datafiles.length
    var done = 0

    log.info("will process " + length + " files")

    datafiles.foreach { path =>
      try {
        val entity = path.getName.replace(".bin", "")

        val is = new FileInputStream(path)
        val batch = new ListBuffer[InsertMessage]()
        var batchCntr = 0

        try {
          val in = CodedInputStream.newInstance(is)
          in.setSizeLimit(1024 << 20) //1 GB

          while (!in.isAtEnd) {
            val tuple = TupleInsertMessage.parseDelimitedFrom(in).get

            val msg = InsertMessage(entity, Seq(tuple))

            batch += msg
            batchCntr += 1

            if (batchCntr > BATCH_SIZE) {
              insertBatch(batch)
              batch.clear()
              batchCntr = 0
            }
          }

          insertBatch(batch)
          batch.clear()
          batchCntr = 0
        } catch {
          case e: Exception =>
            log.error("exception while reading files: " + path, e)
            observer.onNext(AckMessage(AckMessage.Code.ERROR, "error while reading files: " + path))
            insertBatch(batch)
            batch.clear()
            batchCntr = 0
        }

        is.close()

        this.synchronized {
          done += 1
        }
      } catch {
        case e: Exception =>
          log.error("exception while reading files: " + path, e)
          observer.onNext(AckMessage(AckMessage.Code.ERROR, "exception while reading files: " + path))
      }
    }


    def insertBatch(batch: Seq[InsertMessage]): Unit = {
      log.trace("inserting batch of length " + batch.length)

      val inserts = batch.groupBy(_.entity).mapValues(_.flatMap(_.tuples)).map { case (entity, tuples) => InsertMessage(entity, tuples) }.toSeq

      inserts.foreach { insert =>
        val res = Await.result(op(insert), Duration.Inf)
        if (res.code != AckMessage.Code.OK) {
          log.error("exception while inserting files - retrying", res.message)

          //retry to insert each tuple separately
          insert.tuples.foreach { tupleinsert =>
            val subres = InsertMessage(insert.entity, Seq(tupleinsert))

            if (res.code != AckMessage.Code.OK) {
              log.error("exception while inserting tuple: " + subres.toString(), res.message)
            }
          }

        }
        observer.onNext(AckMessage(res.code))
      }

      log.info("status: " + done + "/" + length)
    }

    observer.onCompleted()
  }


  /**
    *
    * @param path
    * @param entity
    */
  def exportData(path: String, entity: Entity): Try[Void] = {
    if (!new File(path).exists() || !new File(path).isDirectory) {
      throw new GeneralAdamException("please specify the path to an existing folder")
    }

    val entityname = entity.entityname

    try {
      writeDataFile(entity.getData().get, new File(path, entityname + ".bin"))
      writeCatalogFile(entityname, entity.schema(fullSchema = false), new File(path, entityname + ".catalog"))

      Success(null)
    } catch {
      case e: Exception => Failure(e)
    }
  }

  /**
    *
    * @param data
    * @param file
    */
  private def writeDataFile(data: DataFrame, file: File): Unit = {
    val cols = data.schema

    /*val messages = data.map(row => {
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
              case _ : ArrayType => DataMessage().withFeatureData(FeatureVectorMessage().withDenseVector(DenseVectorMessage(row.getAs[FeatureVectorWrapper](col.name).toSeq)))
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

    messages.toLocalIterator.foreach { message =>
      message.writeDelimitedTo(fos)
      fos.flush()
    }

    fos.close()*/
  }

  /**
    * @param entityname
    * @param schema
    * @param file
    */
  private def writeCatalogFile(entityname: EntityName, schema: Seq[AttributeDefinition], file: File): Unit = {
    val fos = new FileOutputStream(file)

    def matchFields(ft: AttributeType) = ft match {
      case AttributeTypes.BOOLEANTYPE => grpc.AttributeType.BOOLEAN
      case AttributeTypes.DOUBLETYPE => grpc.AttributeType.DOUBLE
      case AttributeTypes.FLOATTYPE => grpc.AttributeType.FLOAT
      case AttributeTypes.INTTYPE => grpc.AttributeType.INT
      case AttributeTypes.LONGTYPE => grpc.AttributeType.LONG
      case AttributeTypes.STRINGTYPE => grpc.AttributeType.STRING
      case AttributeTypes.TEXTTYPE => grpc.AttributeType.TEXT
      case AttributeTypes.VECTORTYPE => grpc.AttributeType.VECTOR
      case _ => grpc.AttributeType.UNKOWNAT
    }

    val attributes = schema.map(attribute => {
      AttributeDefinitionMessage(attribute.name, matchFields(attribute.attributeType), attribute.params, attribute.storagehandler.name)
    })

    val message = new CreateEntityMessage(entityname, attributes)

    message.writeDelimitedTo(fos)

    fos.flush()
    fos.close()
  }
}
