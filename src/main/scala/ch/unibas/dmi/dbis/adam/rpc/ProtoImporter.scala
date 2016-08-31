package ch.unibas.dmi.dbis.adam.rpc

import java.io.ByteArrayInputStream

import ch.unibas.dmi.dbis.adam.api.EntityOp
import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.exception.GeneralAdamException
import ch.unibas.dmi.dbis.adam.main.AdamContext
import com.google.protobuf.CodedInputStream
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, StructType}
import org.vitrivr.adam.grpc.grpc.InsertMessage.TupleInsertMessage

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * August 2016
  */
class ProtoImporter(entityname: EntityName)(implicit ac: AdamContext) {
  private val BATCH_SIZE = 10000

  private val entity = Entity.load(entityname)
  if (entity.isFailure) {
    throw new GeneralAdamException("cannot load entity")
  }
  private val schema = entity.get.schema()


  /**
    *
    * @param data
    * @return
    */
  def apply(data: Array[Byte]): Try[Void] = {
    try {
      val in = CodedInputStream.newInstance(new ByteArrayInputStream(data))

      val batch = new ListBuffer[Row]()
      var counter = 0

      while (!in.isAtEnd) {
        //parse
        val tuple = TupleInsertMessage.parseFrom(in)
        val data = schema.map(field => {
          val datum = tuple.data.get(field.name).getOrElse(null)
          if (datum != null) {
            RPCHelperMethods.prepareDataTypeConverter(field.fieldtype.datatype)(datum)
          } else {
            null
          }
        })
        batch += Row(data: _*)
        counter += 1

        //if BATCH_SIZE elements collected then insert batch
        if (counter % BATCH_SIZE == 0) {
          insertBatch(batch)
          batch.clear()
        }
      }

      //last batch
      insertBatch(batch)

      Success(null)
    } catch {
      case e: Exception => Failure(e)
    }
  }


  /**
    *
    * @param rows
    */
  private def insertBatch(rows: Seq[Row]): Unit = {
    val rdd = ac.sc.parallelize(rows)
    val df = ac.sqlContext.createDataFrame(rdd, StructType(entity.get.schema().map(field => StructField(field.name, field.fieldtype.datatype))))

    val res = EntityOp.insert(entity.get.entityname, df)

    if (!res.isSuccess) {
      throw res.failed.get
    }
  }

}
