package ch.unibas.dmi.dbis.adam.query.handler.external

import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.handler.generic.{ExpressionDetails, QueryExpression}
import org.apache.http.impl.client.SystemDefaultHttpClient
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.impl.HttpSolrClient
import ch.unibas.dmi.dbis.adam.utils.Logging
import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

/**
  * adampro
  *
  * Ivan Giangreco
  * May 2016
  */
@Experimental class SolrQueryHandler(url: String)(@transient implicit val ac: AdamContext) extends Logging {
  val httpClient = new SystemDefaultHttpClient()
  val client = new HttpSolrClient(url, httpClient)

  //TODO: possibly add a join field
  def query(entityname: EntityName, query: Map[String, String]): DataFrame = {
    val entity = Entity.load(entityname).get
    val pk = entity.pk

    val solrQuery = new SolrQuery();

    if (query.contains("query")) {
      solrQuery.setQuery(query.get("query").get)
    } else {
      solrQuery.setQuery("*:*")
    }

    if (query.contains("filter")) {
      solrQuery.addFilterQuery(query.get("filter").get.split(","): _*)
    }

    val fields: Seq[String] = if (query.contains("fields")) {
      val fields = query.get("fields").get.split(",")

      if (fields.contains(query.getOrElse("pk", pk.name))) {
        fields.drop(fields.indexOf(query.getOrElse("pk", pk.name)))
      } else {
        fields
      }
    } else {
      Seq()
    }
    solrQuery.setFields(fields.+:(query.getOrElse("pk", pk.name)): _*)


    if (query.contains("start")) {
      solrQuery.setStart(query.get("start").get.toInt)
    }

    if (query.contains("defType")) {
      solrQuery.set("defType", query.get("defType").get)
    }

    //TODO: possibly use RDD, do not load all data at once
    import scala.collection.JavaConverters._
    val results = client
      .query(solrQuery)
      .getResults

    log.debug(results.size() + " results retrieved from Solr")

    val rows = results.asScala.toSeq
      .map(doc => {
        Row(
          doc.get(query.getOrElse("pk", pk)),
          fields.map(field => {
            if (doc.containsKey(field)) {
              doc.getFieldValue(field).toString
            } else {
              ""
            }
          })
        )
      })

    val data = ac.sc.parallelize(rows)

    val schema = StructType(
      Seq(StructField(pk.name, entity.pk.fieldtype.datatype, false))
        ++ fields.map(field => StructField(field, StringType, true))
    )

    ac.sqlContext.createDataFrame(data, schema)
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
case class SolrScanExpression(entityname: EntityName, params: Map[String, String], id: Option[String] = None) extends QueryExpression(id) {
  override val info = ExpressionDetails(None, Some("Solr Scan Expression"), id, None)

  override protected def run(filter : Option[DataFrame] = None)(implicit ac: AdamContext): Option[DataFrame] = {
    //TODO: use filter?
    val url = params.get("url").get
    val client = new SolrQueryHandler(url) //possibly cache solr client
    Some(client.query(entityname, params))
  }
}