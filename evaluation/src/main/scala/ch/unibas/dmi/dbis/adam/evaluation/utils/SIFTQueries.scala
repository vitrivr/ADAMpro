package ch.unibas.dmi.dbis.adam.evaluation.utils

import java.io.RandomAccessFile
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.channels.FileChannel

import scala.collection.mutable.ListBuffer

/**
  * Created by silvan on 04.09.16.
  */
object SIFTQueries extends App{

  val queries = getQueries("evaluation/src/main/resources/sift_query.fvecs", 10)
  System.out.println(queries.head.mkString(":"))
  val truths = getTruths("evaluation/src/main/resources/sift_groundtruth.ivecs", 10)
  System.out.println(truths.head.mkString(":"))

  def getQueries(file: String, n: Int): IndexedSeq[Seq[Float]] = {
    val aFile: RandomAccessFile = new RandomAccessFile(file, "rw")
    val inChannel: FileChannel = aFile.getChannel()

    val byteArr = new Array[Byte](516)
    val buf = ByteBuffer.wrap(byteArr)
    buf.order(ByteOrder.LITTLE_ENDIAN)
    var readbytes = -1
    readbytes = inChannel.read(buf)

    val resBuffer = ListBuffer[Seq[Float]]()
    var idx = 0
    while (idx<n && readbytes != (-1) && readbytes == 4 + 128 * 4) {
      val data = new Array[Float](128)
      //buffer is at position 516 after reading, we skip dimensionality-info
      buf.position(4)

      var i = 0
      while(i<128){
        buf.position(4*i+4)
        data(i) = buf.getFloat/512f
        i+=1
      }

      idx+=1
      resBuffer+=data
      buf.clear()
      readbytes = inChannel.read(buf)
    }

    resBuffer.toIndexedSeq
  }

  def getTruths(file: String, n: Int): IndexedSeq[IndexedSeq[Int]] = {
    val aFile: RandomAccessFile = new RandomAccessFile(file, "rw")
    val inChannel: FileChannel = aFile.getChannel()
    //Top 100 Queries
    val d = 100

    //4+100*4
    val byteArr = new Array[Byte](4+4*d)
    val buf = ByteBuffer.wrap(byteArr)
    buf.order(ByteOrder.LITTLE_ENDIAN)
    var readbytes = -1
    readbytes = inChannel.read(buf)
    readbytes = inChannel.read(buf)

    val resBuffer = ListBuffer[IndexedSeq[Int]]()
    var idx = 0

    while (idx < n && readbytes != (-1) && readbytes == 4 + d * 4) {
      val data = new Array[Int](d)

      //buffer is at position 516 after reading, we skip dimensionality-info
      buf.position(4)

      var i = 0
      while (i < d) {
        buf.position(4 * i + 4)
        data(i) = buf.getInt
        i += 1
      }
      idx += 1
      resBuffer += data
      buf.clear()
      readbytes = inChannel.read(buf)
    }

    resBuffer.toIndexedSeq
  }
}
