package ch.unibas.dmi.dbis.adam.client.web.datastructures

import ch.unibas.dmi.dbis.adam.http.grpc.DataMessage.Datatype
import ch.unibas.dmi.dbis.adam.http.grpc.{QueryResultsMessage, QueryResultInfoMessage}

/**
  * adampro
  *
  * Ivan Giangreco
  * April 2016
  */
/**
  *
  * @param intermediateResponses
  */
case class CompoundQueryDetails(intermediateResponses : Seq[QueryResponseInfo]) {
  def this(msg : QueryResultsMessage){
    this(msg.responses.map(ir => new QueryResponseInfo(ir)))
  }
}

/**
  *
  * @param id
  * @param time
  * @param results
  */
case class QueryResponseInfo(id : String, time : Long, results : Seq[Map[String, String]]) {
  def this(msg : QueryResultInfoMessage){
    this(msg.queryid, msg.time, msg.results.map( result => result.data.map(attribute => {
      val key = attribute._1
      val value = attribute._2.datatype match {
        case Datatype.IntData(x) => x.toInt.toString
        case Datatype.LongData(x) => x.toLong.toString
        case Datatype.FloatData(x) => x.toFloat.toString
        case Datatype.DoubleData(x) => x.toDouble.toString
        case Datatype.StringData(x) => x.toString
        case Datatype.BooleanData(x) => x.toString
        case Datatype.FeatureData(x) => x.feature.denseVector.get.vector.mkString("[", ",", "]")
        case _ => ""
      }
      key -> value
    })))
  }
}
