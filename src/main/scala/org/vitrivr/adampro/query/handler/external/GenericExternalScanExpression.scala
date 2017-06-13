package org.vitrivr.adampro.query.handler.external

import org.apache.spark.sql.DataFrame
import org.vitrivr.adampro.entity.Entity
import org.vitrivr.adampro.entity.Entity.EntityName
import org.vitrivr.adampro.exception.GeneralAdamException
import org.vitrivr.adampro.helpers.tracker.OperationTracker
import org.vitrivr.adampro.main.AdamContext
import org.vitrivr.adampro.query.handler.generic.{ExpressionDetails, QueryEvaluationOptions, QueryExpression}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * February 2017
  */
case class GenericExternalScanExpression(entityname: EntityName, handlername : String, params: Map[String, String], id: Option[String] = None)(@transient implicit val ac: AdamContext) extends QueryExpression(id) {
  override val info = ExpressionDetails(None, Some("Generic External Scan Expression"), id, None)

  if (!ac.storageHandlerRegistry.contains(handlername)) {
    throw new GeneralAdamException("no handler '" + handlername + "' found in registry")
  }
  private val handler = {
    assert(ac.storageHandlerRegistry.get(handlername).isDefined)
    ac.storageHandlerRegistry.get(handlername).get
  }

  private val entity = Entity.load(entityname).get

  override protected def run(options : Option[QueryEvaluationOptions], filter: Option[DataFrame] = None)(tracker : OperationTracker)(implicit ac: AdamContext): Option[DataFrame] = {
    val attributes = entity.schema().filter(a => a.storagehandler.equals(handler))
    var status = handler.read(entityname, attributes, params = params)

    if (status.isFailure) {
      throw status.failed.get
    }

    var df = status.get

    if (filter.isDefined) {
      df = df.join(filter.get, entity.pk.name)
    }

    Some(df)
  }
}
