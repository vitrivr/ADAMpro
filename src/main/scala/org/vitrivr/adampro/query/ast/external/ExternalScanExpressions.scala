package org.vitrivr.adampro.query.ast.external

import org.vitrivr.adampro.data.entity.Entity._
import org.vitrivr.adampro.process.SharedComponentContext
import org.vitrivr.adampro.query.ast.generic.QueryExpression

/**
  * adampro
  *
  * Ivan Giangreco
  * May 2016
  */
object ExternalScanExpressions {
  def toQueryExpression(handlername: String, entityname: EntityName, params: Map[String, String], id: Option[String] = None)(implicit ac: SharedComponentContext): QueryExpression = {
    handlername match {
      case _ => new GenericExternalScanExpression(entityname, handlername, params, id)
    }
  }
}
