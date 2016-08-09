package ch.unibas.dmi.dbis.adam.evaluation.io

import java.io._

import ch.unibas.dmi.dbis.adam.http.grpc.{DataMessage, QueryResultTupleMessage}
import net.liftweb.json.Extraction._
import net.liftweb.json._

import scala.collection.mutable.ListBuffer


/**
  * Class for storing QueryResults in JSON
  *
  * Created by silvanheller on 08.08.16.
  */
object QueryResultIO extends App {

  def storeNestedSeq(file: File, data: IndexedSeq[IndexedSeq[Float]], delimiter: String = ":") = {
    var bw: BufferedWriter = null
    if (file.getParentFile != null) {
      file.getParentFile.mkdirs()
    }
    file.delete()
    file.createNewFile()
    try {
      bw = new BufferedWriter(new FileWriter(file))
      var counter = 0
      while (counter < data.size) {
        bw.write(data(counter).mkString(delimiter))
        bw.newLine()
        counter += 1
      }
      bw.flush()
    } finally bw.close()
  }
  def readNestedSeq(file: File, delimiter: String = ":") : IndexedSeq[IndexedSeq[Float]] = {
    val lb = ListBuffer[IndexedSeq[Float]]()
    var br: BufferedReader = null
    try {
      br = new BufferedReader(new FileReader(file))
      var line = br.readLine()
      while (line != null) {
        val q = line.split(delimiter).map(_.toFloat)
        System.out.println(q.head.getClass)
        if (line != "") {
          lb += q
        }
        line = br.readLine()
      }
    } finally br.close()
    lb.toIndexedSeq
  }
  def storeSeq(file: File, topk: IndexedSeq[Float], delimiter: String = ":"): Unit = {
    var bw: BufferedWriter = null
    if (file.getParentFile != null) {
      file.getParentFile.mkdirs()
    }
    file.delete()
    file.createNewFile()
    try {
      bw = new BufferedWriter(new FileWriter(file))
      bw.write(topk.mkString(delimiter))
    } finally bw.close()
  }

  def readSeq[T](file: File, delimiter: String = ":"): IndexedSeq[Float] = {
    var br: BufferedReader = null
    try {
      br = new BufferedReader(new FileReader(file))
      val line = br.readLine()
      line.split(delimiter).map(_.toFloat).toIndexedSeq
    } finally br.close()
  }

  /** DataMessages are Stored with the toString-method and retrieved from Ascii for serialization purposes */
  def fromFile(file: File): IndexedSeq[Seq[QueryResultTupleMessage]] = {
    implicit val formats = net.liftweb.json.DefaultFormats
    val lb = ListBuffer[Seq[QueryResultTupleMessage]]()
    var br: BufferedReader = null
    try {
      br = new BufferedReader(new FileReader(file))
      var line = br.readLine()
      while (line != null) {
        val res = parse(line).extract[List[Map[String, String]]]
        if (line != "") {
          lb += res.map(m => QueryResultTupleMessage(m.map(el => (el._1, DataMessage.fromAscii(el._2)))))
        }
        line = br.readLine()
      }
    } finally br.close()
    lb.toIndexedSeq
  }

  def writeToFile(file: File, data: IndexedSeq[Seq[QueryResultTupleMessage]]): Unit = {
    implicit val formats = net.liftweb.json.DefaultFormats
    var bw: BufferedWriter = null
    if (file.getParentFile != null) {
      file.getParentFile.mkdirs()
    }
    file.delete()
    file.createNewFile()
    try {
      bw = new BufferedWriter(new FileWriter(file))
      var counter = 0
      if (data.size > 2000) System.out.println("Writing data: " + file + " " + data.size)
      while (counter < data.size) {
        val strmaps = data(counter).map(el => el.data.map(el => (el._1, el._2.toString()))).toList
        bw.write(compactRender(decompose(strmaps)))
        bw.newLine()
        counter += 1
      }
      bw.flush()
    } finally bw.close()
  }
}
