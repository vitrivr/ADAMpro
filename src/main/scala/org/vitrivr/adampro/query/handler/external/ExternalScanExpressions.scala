package org.vitrivr.adampro.query.handler.external

import org.vitrivr.adampro.entity.Entity._
import org.vitrivr.adampro.main.AdamContext
import org.vitrivr.adampro.query.handler.generic.QueryExpression

/**
  * adampro
  *
  * Ivan Giangreco
  * May 2016
  */
object ExternalScanExpressions {
  def toQueryExpression(handlername: String, entityname: EntityName, params: Map[String, String], id: Option[String] = None)(implicit ac: AdamContext): QueryExpression = {
    handlername match {
      case _ => new GenericExternalScanExpression(entityname, handlername, params, id)
    }
  }
}
