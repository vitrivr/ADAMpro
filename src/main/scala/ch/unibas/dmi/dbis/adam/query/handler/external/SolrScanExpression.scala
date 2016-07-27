package ch.unibas.dmi.dbis.adam.query.handler.external

import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.exception.GeneralAdamException
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.handler.generic.{QueryEvaluationOptions, ExpressionDetails, QueryExpression}
import ch.unibas.dmi.dbis.adam.storage.handler.StorageHandlerRegistry
import org.apache.spark.sql.DataFrame

/**
  * adampro
  *
  *
  * Ivan Giangreco
  * May 2016
  *
  * @param entityname name of entity
  * @param params     need to specify url and pk
  *                   - query (e.g. "sony digital camera")
  *                   - filter, separated by comma (e.g. "cat:electronics")
  *                   - start, only ints (e.g. 0)
  *                   - defType
  * @param id         query id
  *
  */
case class SolrScanExpression(entityname: EntityName, params: Map[String, String], id: Option[String] = None)(@transient implicit val ac: AdamContext) extends QueryExpression(id) {
  override val info = ExpressionDetails(None, Some("Solr Scan Expression"), id, None)

  if (StorageHandlerRegistry.apply(Some("solr")).isEmpty) {
    throw new GeneralAdamException("no solr handler added")
  }
  private val handler = StorageHandlerRegistry.apply(Some("solr")).get

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