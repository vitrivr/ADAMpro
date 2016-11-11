package org.vitrivr.adampro.query.handler.external

import org.vitrivr.adampro.entity.Entity
import org.vitrivr.adampro.entity.Entity.EntityName
import org.vitrivr.adampro.exception.GeneralAdamException
import org.vitrivr.adampro.main.AdamContext
import org.vitrivr.adampro.query.handler.generic.{QueryEvaluationOptions, ExpressionDetails, QueryExpression}
import org.vitrivr.adampro.storage.StorageHandlerRegistry
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
case class SolrScanExpression(entityname: EntityName, handlername : String, params: Map[String, String], id: Option[String] = None)(@transient implicit val ac: AdamContext) extends QueryExpression(id) {
  override val info = ExpressionDetails(None, Some("Solr Scan Expression"), id, None)

  if (StorageHandlerRegistry.apply(Some("solr")).isEmpty) {
    throw new GeneralAdamException("no solr handler added")
  }
  private val handler = {
    assert(StorageHandlerRegistry.apply(Some(handlername)).isDefined)
    StorageHandlerRegistry.apply(Some(handlername)).get
  }

  private val entity = Entity.load(entityname).get

  override protected def run(options : Option[QueryEvaluationOptions], filter: Option[DataFrame] = None)(implicit ac: AdamContext): Option[DataFrame] = {
    entity.schema().filter(_.storagehandler.get.equals(handler)).map(_.name)

    val attributes = entity.schema().filter(a => a.storagehandler.isDefined && a.storagehandler.get.equals(handler))
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