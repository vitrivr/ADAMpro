package ch.unibas.dmi.dbis.adam.storage.handler

import java.util

import ch.unibas.dmi.dbis.adam.catalog.CatalogOperator
import ch.unibas.dmi.dbis.adam.datatypes.FieldTypes
import ch.unibas.dmi.dbis.adam.datatypes.FieldTypes.FieldType
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.entity.{AttributeDefinition, Entity}
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.utils.Logging
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.impl.HttpSolrClient
import org.apache.solr.client.solrj.request.CoreAdminRequest
import org.apache.solr.common.SolrInputDocument
import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}

import scala.util.{Failure, Success, Try}

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

  private val SOLR_OPTION_NAME = "storing-solr-fieldname"

  /**
    *
    * @param entityname
    * @param attributes
    * @param params
    * @return
    */
  override def create(entityname: EntityName, attributes: Seq[AttributeDefinition], params: Map[String, String])(implicit ac: AdamContext): Try[Void] = {
    try {
      val client = new HttpSolrClient(url)

      val req = CoreAdminRequest.getStatus(entityname, client)
      val cores = (0 until req.getCoreStatus().size()).map { i => req.getCoreStatus().getName(i) }

      val createReq = new CoreAdminRequest.Create()
      createReq.setCoreName(entityname.toString)
      createReq.setInstanceDir(entityname.toString)
      createReq.setConfigSet("basic_configs")
      createReq.process(client)

      attributes.filterNot(_.pk).foreach {
        attribute =>
          CatalogOperator.updateAttributeOption(entityname, attribute.name, SOLR_OPTION_NAME, attribute.name + getSuffix(attribute.fieldtype))
      }

      Success(null)
    } catch {
      case e: Exception => Failure(e)
    }
  }

  /**
    * Returns the dynamic suffix for storing the fieldtype in solr
    * @param fieldtype
    * @return
    */
  private def getSuffix(fieldtype : FieldType) = fieldtype match {
    case FieldTypes.AUTOTYPE => "_l"
    case FieldTypes.INTTYPE => "_i"
    case FieldTypes.LONGTYPE => "_l"
     case FieldTypes.FLOATTYPE => "_f"
    case FieldTypes.DOUBLETYPE => "_d"
    case FieldTypes.STRINGTYPE => "_s"
    case FieldTypes.TEXTTYPE => "_txt"
    case FieldTypes.BOOLEANTYPE => "_b"
  }

  /**
    *
    * @param entityname
    * @param params
    * @return
    */
  override def read(entityname: EntityName, params: Map[String, String])(implicit ac: AdamContext): Try[DataFrame] = {
    val entity = Entity.load(entityname).get
    val schema = entity.schema().filterNot(_.pk)
      .filter(attribute => attribute.storagehandler.isDefined && attribute.storagehandler.get.isInstanceOf[SolrHandler])

    try {
      val solrQuery = new SolrQuery()
      val query = if(params.contains("query")){
        adjustAttributeName(params.get("query").get, schema)
      } else {
        "*:*"
      }
      solrQuery.setQuery(query)

      if (params.contains("filter")) {
        solrQuery.setFilterQueries(params.get("filter").get.split(",").toSeq: _*)
      }
      solrQuery.setRows(Integer.MAX_VALUE) //retrieve all rows

      val client = new HttpSolrClient(url + "/" + entityname.toString)
      val nresults = math.min(Integer.MAX_VALUE, client.query(solrQuery).getResults.getNumFound.toInt)

      val rdd = ac.sc.range(0, nresults).mapPartitions(it => {
        val partClient = new HttpSolrClient(url + "/" + entityname.toString)
        val results = partClient.query(solrQuery).getResults

        it.filter(i => i < results.getNumFound).map(i => results.get(i.toInt)).map(doc => {
          val data = schema.map { attribute => {
            //TODO: adjust for support of other data types
            val strings = doc.get(attribute.params.getOrElse(SOLR_OPTION_NAME, attribute.name)).asInstanceOf[util.ArrayList[String]]

            if (strings != null && strings.size > 0) {
              strings.get(0)
            } else {
              ""
            }
          }
          }

          Row(Seq(doc.get(entity.pk.name)) ++ data: _*)
        })
      })

      //TODO: adjust for support of other data types
      val dfSchema = StructType(Seq(StructField(entity.pk.name + "-str", DataTypes.StringType)) ++ schema.map { case attribute => StructField(attribute.name, attribute.fieldtype.datatype) })
      var df = ac.sqlContext.createDataFrame(rdd, dfSchema)
      df = df.withColumn(entity.pk.name, df.col(entity.pk.name + "-str").cast(entity.pk.fieldtype.datatype))
      df = df.drop(entity.pk.name + "-str")

      Success(df)
    } catch {
      case e: Exception => Failure(e)
    }
  }

  /**
    * Adjusts the query, by replacing the names in front of the : with the dynamic name suffix
    * @param originalQuery
    * @param schema
    * @return
    */
  private def adjustAttributeName(originalQuery : String, schema : Seq[AttributeDefinition]) : String = {
    val schemaMap = schema.map(attribute => attribute.name -> attribute).toMap

    val pattern = "([^:\"']+)|(\"[^\"]*\")|('[^']*')".r
    pattern.findAllIn(originalQuery).zipWithIndex.map{case(txt,idx) => if(idx % 2 == 0){schemaMap.get(txt).map(_.params.getOrElse(SOLR_OPTION_NAME, txt)).getOrElse(txt) + ":"} else {txt}}.mkString
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
    try {
      val entity = Entity.load(entityname).get
      val schema = entity.schema().filterNot(_.pk)
        .filter(attribute => attribute.storagehandler.isDefined && attribute.storagehandler.get.isInstanceOf[SolrHandler])

      df.foreachPartition(pit => {
        val partClient = new HttpSolrClient(url + "/" + entityname.toString)

        pit.foreach { row => {
          val doc = new SolrInputDocument()
          doc.addField("id", row.getAs[Any](entity.pk.name))

          schema.foreach { attribute => {
            doc.addField(attribute.params.getOrElse(SOLR_OPTION_NAME, attribute.name), row.getAs[String](attribute.name))
          }
          }
          partClient.add(doc)
        }
          partClient.commit()
        }
      })

      Success(null)
    } catch {
      case e: Exception =>
        Failure(e)
    }
  }

  /**
    *
    * @param entityname
    * @param params
    * @return
    */
  override def drop(entityname: EntityName, params: Map[String, String])(implicit ac: AdamContext): Try[Void] = {
    try {
      val client = new HttpSolrClient(url)

      client.deleteByQuery(entityname.toString, "*:*")
      client.commit(entityname.toString)

      Success(null)
    } catch {
      case e: Exception => Failure(e)
    }
  }

  override def equals(other: Any): Boolean =
    other match {
      case that: SolrHandler => this.url.equals(that.url)
      case _ => false
    }

  override def hashCode: Int = url.hashCode
}
