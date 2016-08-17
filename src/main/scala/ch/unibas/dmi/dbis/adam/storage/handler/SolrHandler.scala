package ch.unibas.dmi.dbis.adam.storage.handler

import ch.unibas.dmi.dbis.adam.catalog.CatalogOperator
import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.datatypes.FieldTypes
import ch.unibas.dmi.dbis.adam.datatypes.FieldTypes.FieldType
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.entity.{AttributeDefinition, Entity}
import ch.unibas.dmi.dbis.adam.exception.GeneralAdamException
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.utils.Logging
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.impl.HttpSolrClient
import org.apache.solr.client.solrj.request.CoreAdminRequest
import org.apache.solr.common.SolrInputDocument
import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}

import scala.util.{Random, Success, Try}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * July 2016
  */
@Experimental class SolrHandler(private val url: String) extends StorageHandler with Logging with Serializable {
  override val name: String = "storing-solr"

  override def supports = Seq(FieldTypes.AUTOTYPE, FieldTypes.INTTYPE, FieldTypes.LONGTYPE, FieldTypes.FLOATTYPE, FieldTypes.DOUBLETYPE, FieldTypes.STRINGTYPE, FieldTypes.TEXTTYPE, FieldTypes.BOOLEANTYPE)

  override def specializes: Seq[FieldType] = Seq(FieldTypes.TEXTTYPE)

  private val SOLR_OPTION_ENTITYNAME = "storing-solr-corename"
  private val SOLR_OPTION_FIELDNAME = "storing-solr-fieldname"

  /**
    *
    * @param entityname
    */
  private def getCoreName(entityname: EntityName): String = {
    val corename = CatalogOperator.getEntityOption(entityname, Some(SOLR_OPTION_ENTITYNAME)).get.get(SOLR_OPTION_ENTITYNAME)

    if (corename.isEmpty) {
      log.error("corename missing from catalog for entity " + entityname + "; create method has not been called")
      throw new GeneralAdamException("no corename specified in catalog, no fallback")
    }

    corename.get
  }

  /**
    *
    * @param entityname
    * @param attributes
    * @param params
    * @return
    */
  override def create(entityname: EntityName, attributes: Seq[AttributeDefinition], params: Map[String, String])(implicit ac: AdamContext): Try[Void] = {
    execute("create") {
      val client = new HttpSolrClient(url)

      var corename = entityname
      while (exists(corename)) {
        corename = corename + Random.nextInt(999).toString
      }

      CatalogOperator.updateEntityOption(entityname, SOLR_OPTION_ENTITYNAME, corename)

      val createReq = new CoreAdminRequest.Create()
      createReq.setCoreName(corename)
      createReq.setInstanceDir(corename)
      createReq.setConfigSet("basic_configs")
      createReq.process(client)

      attributes.foreach {
        attribute =>
          CatalogOperator.updateAttributeOption(entityname, attribute.name, SOLR_OPTION_FIELDNAME, attribute.name + getSuffix(attribute.fieldtype))
      }

      Success(null)
    }
  }

  /**
    * Check if corename exists already.
    *
    * @param corename
    * @return
    */
  private def exists(corename: String): Boolean = {
    CoreAdminRequest.getStatus(corename, new HttpSolrClient(url)).getCoreStatus(corename).get("instanceDir") != null
  }

  /**
    * Returns the dynamic suffix for storing the fieldtype in solr.
    *
    * @param fieldtype
    * @return
    */
  private def getSuffix(fieldtype: FieldType) = fieldtype match {
    case FieldTypes.AUTOTYPE => "_l"
    case FieldTypes.INTTYPE => "_i"
    case FieldTypes.LONGTYPE => "_l"
    case FieldTypes.FLOATTYPE => "_f"
    case FieldTypes.DOUBLETYPE => "_d"
    case FieldTypes.STRINGTYPE => "_s"
    case FieldTypes.TEXTTYPE => "_txt"
    case FieldTypes.BOOLEANTYPE => "_b"
    case _ => "_s" //in case we do not know how to store the data, choose string
  }

  /**
    *
    * @param entityname
    * @param params
    * @return
    */
  override def read(entityname: EntityName, params: Map[String, String])(implicit ac: AdamContext): Try[DataFrame] = {
    execute("read") {
      val corename = getCoreName(entityname)
      val client = new HttpSolrClient(url + "/" + corename)

      val entity = Entity.load(entityname).get
      val schema = entity.schema().filter(attribute => attribute.storagehandler.isDefined && attribute.storagehandler.get.isInstanceOf[SolrHandler])

      //set query for retrieving data
      val solrQuery = new SolrQuery()
      val query = params.get("query").map(adjustAttributeName(_, schema)).getOrElse("*:*")
      solrQuery.setQuery(query)
      if (params.contains("filter")) {
        solrQuery.setFilterQueries(params.get("filter").get.split(",").toSeq: _*)
      }
      solrQuery.setRows(Integer.MAX_VALUE) //retrieve all rows

      val nresults = math.min(Integer.MAX_VALUE, client.query(solrQuery).getResults.getNumFound.toInt)

      val rdd = ac.sc.range(0, nresults).mapPartitions(it => {
        val results = new HttpSolrClient(url + "/" + corename).query(solrQuery).getResults

        it.filter(i => i < results.getNumFound).map(i => results.get(i.toInt)).map(doc => {
          val data = schema.map { attribute => {
            val fieldData = doc.get(attribute.params.getOrElse(SOLR_OPTION_FIELDNAME, attribute.name))

            if (fieldData != null) {
              fieldData match {
                case list: java.util.ArrayList[String] => if (list.size() > 0) {
                  list.get(0)
                }
                case any => any
              }
            }
          }
          }

          Row(data: _*)
        })
      })

      var df = ac.sqlContext.createDataFrame(rdd, StructType(schema.map(attribute => StructField(attribute.name, attribute.fieldtype.datatype))))
      Success(df)
    }
  }

  /**
    * Adjusts the query, by replacing the names in front of the : with the dynamic name suffix
    *
    * @param originalQuery
    * @param schema
    * @return
    */
  private def adjustAttributeName(originalQuery: String, schema: Seq[AttributeDefinition]): String = {
    val schemaMap = schema.map(attribute => attribute.name -> attribute).toMap

    val pattern = "([^:\"']+)|(\"[^\"]*\")|('[^']*')".r
    pattern.findAllIn(originalQuery).zipWithIndex.map { case (fieldname, idx) =>
      if (idx % 2 == 0) {
        schemaMap.get(fieldname).map(_.params.getOrElse(SOLR_OPTION_FIELDNAME, fieldname)).getOrElse(fieldname) + ":"
      } else {
        fieldname
      }
    }.mkString
  }


  /**
    *
    * @param entityname
    * @param df
    * @param mode
    * @param params
    * @return
    */
  override def write(entityname: EntityName, df: DataFrame, mode: SaveMode, params: Map[String, String])(implicit ac: AdamContext): Try[Void] = {
    execute("write") {
      val corename = getCoreName(entityname)

      val entity = Entity.load(entityname).get
      val schema = entity.schema().filter(attribute => attribute.storagehandler.isDefined && attribute.storagehandler.get.isInstanceOf[SolrHandler])

      df.foreachPartition { it =>
        val partClient = new HttpSolrClient(url + "/" + corename)

        it.foreach { row =>
          val doc = new SolrInputDocument()
          doc.addField(FieldNames.internalIdColumnName, row.getAs[Any](entity.pk.name))

          schema.foreach { attribute =>
            doc.addField(attribute.params.getOrElse(SOLR_OPTION_FIELDNAME, attribute.name), row.getAs[String](attribute.name))
          }
          partClient.add(doc)
        }

        partClient.commit()
      }

      Success(null)
    }
  }

  /**
    *
    * @param entityname
    * @param params
    * @return
    */
  override def drop(entityname: EntityName, params: Map[String, String])(implicit ac: AdamContext): Try[Void] = {
    execute("drop") {
      val corename = getCoreName(entityname)

      val client = new HttpSolrClient(url)

      client.deleteByQuery(entityname.toString, "*:*")
      client.commit(corename)

      //deleting core is not easily possible, therefore we just delete the data

      Success(null)
    }
  }

  override def equals(other: Any): Boolean =
    other match {
      case that: SolrHandler => this.url.equals(that.url)
      case _ => false
    }

  override def hashCode: Int = url.hashCode
}
