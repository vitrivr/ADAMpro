package org.vitrivr.adampro.rpc.datastructures

import org.vitrivr.adampro.grpc.grpc.DataMessage.Datatype
import org.vitrivr.adampro.grpc.grpc.VectorMessage.Vector

import org.vitrivr.adampro.grpc.grpc.QueryResultInfoMessage

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * July 2016
  */
case class RPCQueryResults(id: String, time: Long, source : String = "", info : Map[String, String] = Map(), confidence : Double = 0, results: Seq[Map[String, String]]) {
  def this(msg: QueryResultInfoMessage) {
    this(msg.queryid, msg.time, msg.source, msg.info, msg.confidence, msg.results.map(result => result.data.map(attribute => {
      val key = attribute._1
      val value = attribute._2.datatype match {
        case Datatype.IntData(x) => x.toInt.toString
        case Datatype.LongData(x) => x.toLong.toString
        case Datatype.FloatData(x) => x.toFloat.toString
        case Datatype.DoubleData(x) => x.toDouble.toString
        case Datatype.StringData(x) => x.toString
        case Datatype.BooleanData(x) => x.toString
        case Datatype.VectorData(x) => {
          x.vector match {
            case Vector.DenseVector(y) => y.vector.mkString("[", ",", "]")
            case Vector.SparseVector(y) => y.index.zip(y.data).map{case(index, data) => "(" + index + "," + data + ")"}.mkString("[", ",", "]")
            case Vector.IntVector(y) => y.vector.mkString("[", ",", "]")
          }
        }
        case _ => ""
      }
      key -> value
    })))
  }
}