package ch.unibas.dmi.dbis.adam.evaluation.io

import java.io._

import ch.unibas.dmi.dbis.adam.http.grpc.DenseVectorMessage

import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
  * IO for FeatureVectors (Seq[Float] to be specific)
  *
  * Created by silvanheller on 08.08.16.
  */
object FeatureVectorIO extends App {

  //Testing
  val data = IndexedSeq.tabulate(10)(_ => IndexedSeq.fill(20)(Random.nextFloat()))
  FeatureVectorIO.toFile(new File("resources/test.qv"), data)
  val read = FeatureVectorIO.fromFile(new File("resources/test.qv"))
  if(data!=read){
    System.err.println("fail")
  } else System.out.println("Heureka! ")


  //TODO Support Only reading certain Lines
  //TODO Switch to JSON Maybe

  /**
    * Reads Float-Seqs from a File
    * Lines are read one-by one
    * @param delimiter default is , without space
    * @return
    */
  def fromFile(file: File, delimiter:String=",") : IndexedSeq[IndexedSeq[Float]] = {
    val lb = ListBuffer[IndexedSeq[Float]]()
    var br : BufferedReader = null
    try{
      br = new BufferedReader(new FileReader(file))
      var line = br.readLine()
      while(line!=null){
        val q = line.split(delimiter).map(f => f.toFloat)
        if(line!=""){
          lb+=q
        }
        line = br.readLine()
      }
    }finally br.close

    lb.toIndexedSeq
  }

  /**
    * Writes your list of float-seqs to a file. It is guaranteed that the seqs are stored in the order that they are entered
    * since we use a while-loop and data is an indexedSeq
    * @param data
    */
  def toFile(file: File, data: IndexedSeq[IndexedSeq[Float]]) : Unit = {
    var bw : BufferedWriter = null
    if(!file.exists()){
      if (file.getParentFile() != null) {
        file.getParentFile().mkdirs()
      }
      file.createNewFile()
      System.out.println("Created File: "+file.getName)
    }
    try{
      bw = new BufferedWriter(new FileWriter(file))
      var counter = 0
      while(counter<data.size){
        bw.write(data(counter).mkString(","))
        bw.newLine()
        counter+=1
      }
      bw.flush()
    }finally bw.close
  }
}
