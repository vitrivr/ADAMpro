package org.vitrivr.adampro.storage.engine

import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.impl.HttpSolrClient
import org.apache.solr.client.solrj.request.CoreAdminRequest
import org.apache.solr.common.SolrInputDocument
import org.apache.spark.sql.types.{FloatType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.vitrivr.adampro.config.AttributeNames
import org.vitrivr.adampro.data.datatypes.AttributeTypes
import org.vitrivr.adampro.data.datatypes.AttributeTypes.AttributeType
import org.vitrivr.adampro.data.entity.AttributeDefinition
import org.vitrivr.adampro.data.entity.Entity.AttributeName
import org.vitrivr.adampro.process.SharedComponentContext
import org.vitrivr.adampro.query.query.Predicate
import org.vitrivr.adampro.utils.Logging

import scala.util.{Failure, Success, Try}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * September 2016
  */
class SolrEngine(private val url: String)(@transient override implicit val ac: SharedComponentContext) extends Engine()(ac) with Logging with Serializable {
  override val name: String = "solr"

  override def supports = Seq(AttributeTypes.AUTOTYPE, AttributeTypes.INTTYPE, AttributeTypes.LONGTYPE, AttributeTypes.FLOATTYPE, AttributeTypes.DOUBLETYPE, AttributeTypes.STRINGTYPE, AttributeTypes.TEXTTYPE, AttributeTypes.BOOLEANTYPE)

  override def specializes: Seq[AttributeType] = Seq(AttributeTypes.TEXTTYPE)

  override val repartitionable = false

  private val SOLR_OPTION_ENTITYNAME = "storing-solr-corename"
  private val SOLR_OPTION_FIELDNAME = "storing-solr-fieldname"

  private val SOLR_MAX_RESULTS = 5000


  /**
    *
    * @param props
    */
  def this(props: Map[String, String])(implicit ac: SharedComponentContext) {
    this(props.get("url").get)(ac)
  }


  /**
    *
    * @param baseUrl
    * @param storename
    */
  private def getClient(baseUrl: String, storename: Option[String] = None): HttpSolrClient = {
    val url = baseUrl + storename.map("/" + _).getOrElse("")

    new HttpSolrClient.Builder().withBaseSolrUrl(url).build()
  }


  /**
    * Create the entity.
    *
    * @param storename  adapted entityname to store feature to
    * @param attributes attributes of the entity (w.r.t. handler)
    * @param params     creation parameters
    * @return
    */
  override def create(storename: String, attributes: Seq[AttributeDefinition], params: Map[String, String])(implicit ac: SharedComponentContext): Try[Map[String, String]] = {
    val client = getClient(url)

    try {
      val createReq = new CoreAdminRequest.Create()
      createReq.setCoreName(storename)
      createReq.setInstanceDir(storename)
      createReq.setConfigSet("basic_configs")
      createReq.process(client)

      Success(Map())
    } catch {
      case e: Exception =>
        Failure(e)
    }
  }

  /**
    * Check if entity exists.
    *
    * @param storename adapted entityname to store feature to
    * @return
    */
  override def exists(storename: String)(implicit ac: SharedComponentContext): Try[Boolean] = {
    log.trace("solr exists operation")

    try {
      Success(CoreAdminRequest.getStatus(storename, getClient(url)).getCoreStatus(storename).get("instanceDir") != null)
    } catch {
      case e: Exception =>
        Failure(e)
    }
  }

  /**
    * Returns the dynamic suffix for storing the attribute type in solr.
    *
    * @param attributetype
    * @return
    */
  private def getSuffix(attributetype: AttributeType) = attributetype match {
    case AttributeTypes.INTTYPE => "_i"
    case AttributeTypes.LONGTYPE => "_l"
    case AttributeTypes.AUTOTYPE => "_l"
    case AttributeTypes.FLOATTYPE => "_f"
    case AttributeTypes.DOUBLETYPE => "_d"
    case AttributeTypes.STRINGTYPE => "_s"
    case AttributeTypes.TEXTTYPE => "_txt"
    case AttributeTypes.BOOLEANTYPE => "_b"
    case _ => "_s" //in case we do not know how to store the data, choose string
  }


  /**
    * Returns the attribute type given a solr suffix.
    *
    * @param suffix
    * @return
    */
  private def getAttributeType(suffix: String) = suffix match {
    case "_i" => AttributeTypes.INTTYPE
    case "_l" => AttributeTypes.LONGTYPE
    case "_f" => AttributeTypes.FLOATTYPE
    case "_d" => AttributeTypes.DOUBLETYPE
    case "_s" => AttributeTypes.STRINGTYPE
    case "_txt" => AttributeTypes.TEXTTYPE
    case "_b" => AttributeTypes.BOOLEANTYPE
    case _ => AttributeTypes.STRINGTYPE
  }

  /**
    * Read entity.
    *
    * @param storename  adapted entityname to store feature to
    * @param attributes the attributes to read
    * @param predicates filtering predicates (only applied if possible)
    * @param params     reading parameters
    * @return
    */
  override def read(storename: String, attributes: Seq[AttributeDefinition], predicates: Seq[Predicate], params: Map[String, String])(implicit ac: SharedComponentContext): Try[DataFrame] = {
    try {
      val client = getClient(url, Some(storename))
      val nameDicAttributenameToSolrname = attributes.map(attribute => attribute.name -> (attribute.name + getSuffix(attribute.attributeType))).toMap
      val nameDicSolrnameToAttributename = nameDicAttributenameToSolrname.map(_.swap)

      //set query for retrieving data
      val solrQuery = new SolrQuery()
      val query = params.get("query").map(adjustAttributeName(_, nameDicAttributenameToSolrname)).getOrElse("*:*")
      solrQuery.setQuery(query)
      if (params.contains("filter")) {
        solrQuery.setFilterQueries(params.get("filter").get.split(",").toSeq: _*)
      }
      solrQuery.setFields("*", "score")
      solrQuery.setRows(SOLR_MAX_RESULTS)
      solrQuery.setSort("score", SolrQuery.ORDER.desc)
      solrQuery.set("defType", "edismax")

      //set all other params
      (params - ("query", "fields")).foreach { case (param, value) =>
        solrQuery.set(param, value)
      }

      val results = client.query(solrQuery).getResults
      val nresults = results.size()

      log.trace("solr returns " + nresults + " results")

      if (results.size > 0) {
        log.trace("solr returns fields " + results.get(0).getFieldNames.toArray.mkString(","))
      } else {
        log.trace("solr returns 0 results")
      }

      import collection.JavaConverters._
      val rdd = ac.sqlContext.sparkContext.parallelize(results.subList(0, nresults).asScala.map(doc => {
        val data = (nameDicSolrnameToAttributename.keys.toSeq ++ Seq("score")).map(solrname => {
          val fieldData = doc.get(solrname)

          if (fieldData != null) {
            fieldData match {
              case list: java.util.ArrayList[_] => if (list.size() > 0) {
                list.get(0)
              }
              case any => any
            }
          } else {
            null
          }
        }).filter(_ != null).toSeq
        Row(data: _*)
      }))

      val df = if (!results.isEmpty) {
        val tmpDoc = results.get(0)
        val schema = (nameDicSolrnameToAttributename.keys.toSeq ++ Seq("score")).map(solrname => {
          if (solrname == "score") {
            StructField(AttributeNames.scoreColumnName, FloatType)
          } else if (tmpDoc.get(solrname) != null) {
            val name = nameDicSolrnameToAttributename(solrname)
            val attributetype = getAttributeType(solrname.substring(solrname.lastIndexOf("_")))
            StructField(name, attributetype.datatype)
          } else {
            null
          }
        }).filter(_ != null)
        ac.sqlContext.createDataFrame(rdd, StructType(schema))
      } else {
        ac.sqlContext.emptyDataFrame
      }

      Success(df)
    }

    catch {
      case e: Exception =>
        log.error("fatal error when reading from solr", e)
        Failure(e)
    }
  }

  /**
    * Adjusts the query, by replacing the names in front of the : with the dynamic name suffix
    *
    * @param originalQuery
    * @param nameDic
    * @return
    */
  private def adjustAttributeName(originalQuery: String, nameDic: Map[AttributeName, String]): String = {

    val pattern = "([^:\"']+)|(\"[^\"]*\")|('[^']*')".r
    pattern.findAllIn(originalQuery).zipWithIndex.map {
      case (fieldname, idx) =>
        if (idx % 2 == 0) {
          val solrname = nameDic.get(fieldname)

          if (solrname.isDefined) {
            solrname.get + ":"
          } else {
            log.error("field " + fieldname + " not stored in solr")
          }

        } else {
          fieldname
        }
    }.mkString
  }


  /**
    * Write entity.
    *
    * @param storename  adapted entityname to store feature to
    * @param df         data
    * @param attributes attributes to store
    * @param mode       save mode (append, overwrite, ...)
    * @param params     writing parameters
    * @return new options to store
    */
  override def write(storename: String, df: DataFrame, attributes: Seq[AttributeDefinition], mode: SaveMode = SaveMode.Append, params: Map[String, String])(implicit ac: SharedComponentContext): Try[Map[String, String]] = {
    val pk = attributes.filter(_.pk).head

    df.foreachPartition {
      it =>
        val partClient = getClient(url, Some(storename))

        it.foreach {
          row =>
            val doc = new SolrInputDocument()
            doc.addField("id", row.getAs[Any](pk.name))

            attributes.foreach {
              attribute =>
                val solrname = attribute.name + getSuffix(attribute.attributeType)
                doc.addField(solrname, row.getAs[Any](attribute.name).toString)
            }

            partClient.add(doc)
        }

        partClient.commit()
    }

    Success(Map())
  }

  /**
    * Drop the entity.
    *
    * @param storename adapted entityname to store feature to
    * @return
    */
  override def drop(storename: String)(implicit ac: SharedComponentContext): Try[Void] = {
    val client = getClient(url, Some(storename))
    client.deleteByQuery(storename.toString, "*:*")
    client.commit(storename)

    try {
      val unloadReq = new CoreAdminRequest.Unload(true)
      unloadReq.setDeleteDataDir(true)
      unloadReq.setDeleteIndex(true)
      unloadReq.setDeleteInstanceDir(true)
      unloadReq.process(client)
      Success(null)
    } catch {
      case e: Exception => Failure(e)
    }
  }
}
