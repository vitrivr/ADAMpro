package ch.unibas.dmi.dbis.adam.client.web.datastructures

/**
  * adampro
  *
  * Ivan Giangreco
  * April 2016
  */
case class ProgressiveQueryRequest(val id : String, val entityname : String, query : String, hints : Seq[String], val k : Int) {
  def q = query.split(",").map(_.toFloat)
}

case class ProgressiveQueryTemporaryResultsRequest(id: String)

