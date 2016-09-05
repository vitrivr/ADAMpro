package ch.unibas.dmi.dbis.adam.evaluation.utils

import java.io._
import java.nio.channels.FileChannel
import java.nio.{ByteBuffer, ByteOrder}

import com.google.common.util.concurrent.SettableFuture
import io.grpc.stub.StreamObserver
import org.vitrivr.adam.grpc.grpc.AdamDefinitionGrpc.AdamDefinitionStub
import org.vitrivr.adam.grpc.grpc.InsertMessage.TupleInsertMessage
import org.vitrivr.adam.grpc.grpc._

import scala.collection.mutable.ListBuffer
import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
  * Created by silvan on 04.09.16.
  */
object VecImporter extends Logging {

  def genEntity(definer: AdamDefinitionStub): Unit = {
    val eName = "sift_realdata"
    val preview = Await.result(definer.getEntityProperties(EntityNameMessage("sift_realdata")), Duration(100, "s"))
    System.out.println(preview)

    //Await.result(definer.dropEntity(EntityNameMessage("sift_realdata")), Duration(100, "s"))
    val exists = Await.result(definer.listEntities(EmptyMessage()), Duration(100, "s")).entities.find(_.equals(eName))
    if (exists.isEmpty) {
      log.info("Generating new Entity: " + eName)
      definer.createEntity(CreateEntityMessage(eName, Seq(AttributeDefinitionMessage.apply("pk", AttributeType.LONG, pk = true, unique = true, indexed = true),
        AttributeDefinitionMessage("feature", AttributeType.FEATURE, pk = false, unique = false, indexed = true))))
      val options = Map("fv-dimensions" -> 128, "fv-min" -> 0, "fv-max" -> 1, "fv-sparse" -> false).mapValues(_.toString)
    } else log.info("Using existing entity: " + eName)
  }

  def importVec(fileName: String, definer: AdamDefinitionStub): Unit = {
    var id = 0l

    val aFile: RandomAccessFile = new RandomAccessFile(fileName, "rw")
    val inChannel: FileChannel = aFile.getChannel()

    val tuples = new ListBuffer[TupleInsertMessage]()

    val byteArr = new Array[Byte](516)
    val buf = ByteBuffer.wrap(byteArr)
    buf.order(ByteOrder.LITTLE_ENDIAN)
    var readbytes = -1
    var insertIdx = 0
    readbytes = inChannel.read(buf)

    var future: SettableFuture[AckMessage] = SettableFuture.create()
    var insertObserver: StreamObserver[InsertMessage] = definer.insert(new LastAckStreamObserver(future))

    val tic = System.currentTimeMillis()
    while (readbytes != (-1) && readbytes == 4 + 128 * 4) {
      val data = new Array[Float](128)
      //buffer is at position 516 after reading, we skip dimensionality-info
      buf.position(4)
      //val data = new Array[Float](128)
      //asFloatBuffer() doesn't work

      var i = 0
      while(i<128){
        buf.position(4*i+4)
        data(i) = buf.getFloat/512f
        i+=1
      }
      tuples+=TupleInsertMessage().withData(Map("pk" -> DataMessage().withLongData(id), "feature" -> DataMessage().withFeatureData(FeatureVectorMessage().withDenseVector(DenseVectorMessage().withVector(data)))))
      //TODO add to buffer
      insertIdx += 1
      id+=1
      if (insertIdx % 40000 == 0) {
        //TODO Copy tuples??
        insertObserver.onNext(InsertMessage.apply("sift_realdata", tuples.toSeq))

        insertObserver.onCompleted()
        val ack = future.get()
        System.out.println(ack.message)

        future = SettableFuture.create()
        insertObserver = definer.insert(new LastAckStreamObserver(future))

        tuples.clear()
        val toc = System.currentTimeMillis()
        log.info("Completed in {} ms, current id: {}", toc-tic, id)
        if(id==120000){
          insertObserver.onCompleted()
          future.get()
          System.exit(1)
        }
      }
      //reset buffer
      buf.clear()
      readbytes = inChannel.read(buf)
    }

    val toc = System.currentTimeMillis()
    log.info("Completed in {} ms", toc-tic)
    insertObserver.onCompleted()
    future.get()

    val preview = Await.result(definer.getEntityProperties(EntityNameMessage("sift_realdata")), Duration(10, "s"))
    System.out.println(preview)
  }
}

/** Thanks to luca's cineast code :) This should make insert work in a stream-ish way*/
class LastAckStreamObserver(final val future: SettableFuture[AckMessage]) extends StreamObserver[AckMessage]{

  private var last : AckMessage = null
  override def onCompleted(): Unit = {
    future.set(this.last)
  }

  override def onError(throwable: Throwable): Unit = {
    throwable.printStackTrace()
    System.out.println(throwable.getLocalizedMessage)
    future.setException(throwable)
  }

  override def onNext(v: AckMessage): Unit = {
    this.last = v
  }
}
