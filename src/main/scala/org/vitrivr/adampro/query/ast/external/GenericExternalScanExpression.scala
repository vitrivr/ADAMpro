package org.vitrivr.adampro.query.ast.external

import org.apache.spark.sql.DataFrame
import org.vitrivr.adampro.data.entity.Entity
import org.vitrivr.adampro.data.entity.Entity.EntityName
import org.vitrivr.adampro.utils.exception.GeneralAdamException
import org.vitrivr.adampro.process.SharedComponentContext
import org.vitrivr.adampro.query.tracker.QueryTracker
import org.vitrivr.adampro.query.ast.generic.{ExpressionDetails, QueryEvaluationOptions, QueryExpression}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * February 2017
  */
case class GenericExternalScanExpression(entityname: EntityName, handlername : String, params: Map[String, String], id: Option[String] = None)(@transient implicit val ac: SharedComponentContext) extends QueryExpression(id) {
  override val info = ExpressionDetails(None, Some("Generic External Scan Expression"), id, None)

  if (!ac.storageManager.contains(handlername)) {
    throw new GeneralAdamException("no handler '" + handlername + "' found in registry")
  }
  private val handler = {
    assert(ac.storageManager.get(handlername).isDefined)
    ac.storageManager.get(handlername).get
  }

  private val entity = Entity.load(entityname).get

  override protected def run(options : Option[QueryEvaluationOptions], filter: Option[DataFrame] = None)(tracker : QueryTracker)(implicit ac: SharedComponentContext): Option[DataFrame] = {
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
