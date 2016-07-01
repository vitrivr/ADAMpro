package ch.unibas.dmi.dbis.adam.query.handler.external

import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.handler.generic.{ExpressionDetails, QueryExpression}
import ch.unibas.dmi.dbis.adam.utils.Logging
import org.apache.http.impl.client.SystemDefaultHttpClient
import org.apache.solr.client.solrj.impl.HttpSolrClient
import org.apache.solr.common.SolrInputDocument
import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.DataFrame

import scala.util.{Success, Try}

/**
  * adampro
  *
  * Ivan Giangreco
  * May 2016
  */
@Experimental class SolrQueryHandler(url: String)(@transient implicit val ac: AdamContext) extends ExternalQueryHandler with Logging {
  val client = new HttpSolrClient(url, new SystemDefaultHttpClient())

  override def key: String = "solr-" + url

  override def query(entityname: EntityName, query: Map[String, String], joinfield: Option[String]): DataFrame = {
    val entity = Entity.load(entityname).get
    val pk = entity.pk

    var df = ac.sqlContext.read.format("solr").options(query).load

    if (joinfield.isDefined && !pk.name.equals(joinfield.get)) {
      df = df.withColumnRenamed(joinfield.get, pk.name)
    }

    log.debug("results retrieved from solr")
    df
  }

  override def insert(data: DataFrame, params: Map[String, String]): Try[Void] = {
    val core = params.get("core").get
    data.foreach({ datum =>
      val doc = new SolrInputDocument()

      datum.schema.fields.foreach { attribute =>
        doc.addField(attribute.name, datum.getAs[Any](attribute.name))
      }

      log.trace("datum inserted into solr")
      client.add(doc)
    })
    client.commit()

    Success(null)
  }
}

object SolrQueryHandler {
  def getInsertionHandler(params: Map[String, String])(implicit ac: AdamContext): ExternalQueryHandler = {
    new SolrQueryHandler(params.get("url").get) //possibly cache solr client
  }
}

/**
  *
  * @param entityname
  * @param params need to specify url and pk
  *               - url, include core (e.g. http://192.168.99.100:32769/solr/adampro where adampro is the core name)
  *               - query (e.g. "sony digital camera")
  *               - filter, separated by comma (e.g. "cat:electronics")
  *               - fields (e.g. "id,company,cat")
  *               - pk field (e.g. "id")
  *               - start, only ints (e.g. 0)
  *               - defType
  * @param id
  */
case class SolrScanExpression(entityname: EntityName, params: Map[String, String], joinfield: Option[String] = None, id: Option[String] = None) extends QueryExpression(id) {
  override val info = ExpressionDetails(None, Some("Solr Scan Expression"), id, None)

  override protected def run(filter: Option[DataFrame] = None)(implicit ac: AdamContext): Option[DataFrame] = {
    val client = new SolrQueryHandler(params.get("url").get) //possibly cache solr client
    var df = client.query(entityname, params, joinfield)

    if (filter.isDefined) {
      val entity = Entity.load(entityname).get
      val pk = entity.pk

      df = df.join(filter.get, pk.name)
    }

    Some(df)
  }
}