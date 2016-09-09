package ch.unibas.dmi.dbis.adam.query.handler.external

import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.handler.generic.{ExpressionDetails, QueryEvaluationOptions, QueryExpression}
import ch.unibas.dmi.dbis.adam.storage.StorageHandlerRegistry
import org.apache.spark.sql.DataFrame

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * September 2016
  */
case class GisScanExpression(entityname: EntityName, handlername : String, params: Map[String, String], id: Option[String] = None)(@transient implicit val ac: AdamContext) extends QueryExpression(id) {
  override val info = ExpressionDetails(None, Some("Gis Scan Expression"), id, None)

  private val handler = StorageHandlerRegistry.apply(Some(handlername)).get

  private val entity = Entity.load(entityname).get

  override protected def run(options : Option[QueryEvaluationOptions], filter: Option[DataFrame] = None)(implicit ac: AdamContext): Option[DataFrame] = {
    var status = handler.read(entityname, params)

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
