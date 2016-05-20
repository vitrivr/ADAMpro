package ch.unibas.dmi.dbis.adam.query.handler.external

import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.datastructures.QueryExpression

/**
  * adampro
  *
  * Ivan Giangreco
  * May 2016
  */
object ExternalHandlers {
  def toQueryExpression(handler : String, entityname : EntityName, params : Map[String, String], id: Option[String] = None)(implicit ac: AdamContext) : QueryExpression = handler match {
    case "solr" => SolrQueryHolder(entityname, params, id)
    case _ => null
  }
}
