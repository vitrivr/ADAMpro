package ch.unibas.dmi.dbis.adam.evaluation.io

import java.io._

import scala.collection.mutable.ListBuffer


/**
  * Class for storing Stuff
  *
  * Created by silvanheller on 08.08.16.
  */
object SeqIO extends App {

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

  def readSeq(file: File, delimiter: String = ":"): IndexedSeq[Float] = {
    var br: BufferedReader = null
    try {
      br = new BufferedReader(new FileReader(file))
      val line = br.readLine()
      line.split(delimiter).map(_.toFloat).toIndexedSeq
    } finally br.close()
  }
}
