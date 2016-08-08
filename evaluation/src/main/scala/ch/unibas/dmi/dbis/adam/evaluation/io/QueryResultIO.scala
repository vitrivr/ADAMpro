package ch.unibas.dmi.dbis.adam.evaluation.io

import java.io._

import adampro.grpc.shaded.com.google.protobuf.Descriptors
import ch.unibas.dmi.dbis.adam.http.grpc.{DataMessage, QueryResultTupleMessage}
import net.liftweb.json.JsonAST.JValue

import scala.collection.mutable.ListBuffer
import scala.util.Random
import net.liftweb.json.{JValue, _}
import net.liftweb.json.Extraction._


/**
  * Class for storing QueryResults in JSON
  *
  * Includes a little bit of testing code at the beginning to show that it works
  *
  * Created by silvanheller on 08.08.16.
  */
object QueryResultIO extends App{
  implicit val formats = net.liftweb.json.DefaultFormats

  val qData = IndexedSeq.tabulate(20)(outer => Seq.tabulate(20)(el => QueryResultTupleMessage(Map("ap_distance" -> DataMessage().withIntData(el*outer), "information" -> DataMessage().withStringData("hello")))))
  toFile(new File("resources/test.qrs"), qData)
  val qRead = fromFile(new File("resources/test.qrs"))

  if(qData!= qRead){
    System.err.println("fail")
  } else System.out.println("heureka! ")

  /**
    * DataMessages are Stored with the toString-method and retrieved from Ascii for serialization purposes
    *
    * @param file
    * @return
    */
  def fromFile(file: File) : IndexedSeq[Seq[QueryResultTupleMessage]] = {
    val lb = ListBuffer[Seq[QueryResultTupleMessage]]()
    var br : BufferedReader = null
    try{
      br = new BufferedReader(new FileReader(file))
      var line = br.readLine()
      while(line!=null){
        val res = parse(line).extract[List[Map[String, String]]]
        if(line!=""){
          lb+=res.map(m => QueryResultTupleMessage(m.map(el => (el._1, DataMessage.fromAscii(el._2)))))
        }
        line = br.readLine()
      }
    }finally br.close
    lb.toIndexedSeq
  }

  def toFile(file: File, data: IndexedSeq[Seq[QueryResultTupleMessage]]) : Unit = {
    var bw : BufferedWriter = null
    if(!file.exists()){
      if (file.getParentFile() != null) {
        file.getParentFile().mkdirs()
      }
      file.createNewFile()
      System.out.println("Created File"+file.getName)
    }
    try{
      bw = new BufferedWriter(new FileWriter(file))
      var counter = 0
      while(counter<data.size){
        val strmaps = data(counter).map(el => el.data.map(el => (el._1, el._2.toString()))).toList
        bw.write(compactRender((decompose(strmaps))))
        bw.newLine()
        counter+=1
      }
      bw.flush()
    }finally bw.close
  }
}
