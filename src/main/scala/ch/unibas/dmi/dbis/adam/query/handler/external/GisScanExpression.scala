package ch.unibas.dmi.dbis.adam.query.handler.external

import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.exception.GeneralAdamException
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.handler.generic.{QueryEvaluationOptions, ExpressionDetails, QueryExpression}
import ch.unibas.dmi.dbis.adam.storage.handler.StorageHandlerRegistry
import org.apache.spark.sql.DataFrame

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * September 2016
  */
case class GisScanExpression(entityname: EntityName, params: Map[String, String], id: Option[String] = None)(@transient implicit val ac: AdamContext) extends QueryExpression(id) {
  override val info = ExpressionDetails(None, Some("Gis Scan Expression"), id, None)

  if (StorageHandlerRegistry.apply(Some("gis")).isEmpty) {
    throw new GeneralAdamException("no gis handler added")
  }
  private val handler = StorageHandlerRegistry.apply(Some("gis")).get

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
