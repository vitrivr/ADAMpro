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
  override val name: String = "solr"

  override def supports = Seq(FieldTypes.AUTOTYPE, FieldTypes.INTTYPE, FieldTypes.LONGTYPE, FieldTypes.STRINGTYPE, FieldTypes.TEXTTYPE)

  override def specializes: Seq[FieldType] = Seq(FieldTypes.TEXTTYPE)

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

      attributes.foreach {
        attribute =>
          CatalogOperator.updateAttributeOption(entityname, attribute.name, "solrfieldname", attribute.name + "_txt")
      }

      Success(null)
    } catch {
      case e: Exception => Failure(e)
    }
  }

  override def read(entityname: EntityName, params: Map[String, String])(implicit ac: AdamContext): Try[DataFrame] = {
    val entity = Entity.load(entityname).get
    val schema = entity.schema().filterNot(_.pk)
      .filter(attribute => attribute.storagehandler.isDefined && attribute.storagehandler.get.isInstanceOf[SolrHandler])
      .map(attribute => CatalogOperator.getAttributeOption(entityname, name, "solrfieldname") -> attribute)

    try {
      val query = new SolrQuery()
      query.setQuery(params.getOrElse("query", "*:*"))
      if (params.contains("filter")) {
        query.setFilterQueries(params.get("filter").get.split(",").toSeq: _*)
      }
      query.setRows(Integer.MAX_VALUE)

      val client = new HttpSolrClient(url + "/" + entityname.toString)
      val nresults = math.min(Integer.MAX_VALUE, client.query(query).getResults.getNumFound.toInt)

      val rdd = ac.sc.range(0, nresults).mapPartitions(it => {
        val partClient = new HttpSolrClient(url + "/" + entityname.toString)
        val results = partClient.query(query).getResults

        it.filter(i => i < results.getNumFound).map(i => results.get(i.toInt)).map(doc => {
          val data = schema.map { case (name, attribute) => {
            val strings = doc.get(name.getOrElse(attribute.name)).asInstanceOf[util.ArrayList[String]]

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

      val dfSchema = StructType(Seq(StructField(entity.pk.name + "-str", DataTypes.StringType)) ++ schema.map { case (name, attribute) => StructField(attribute.name, attribute.fieldtype.datatype) })
      var df = ac.sqlContext.createDataFrame(rdd, dfSchema)
      df = df.withColumn(entity.pk.name, df.col(entity.pk.name + "-str").cast(entity.pk.fieldtype.datatype))
      df = df.drop(entity.pk.name + "-str")

      Success(df)
    } catch {
      case e: Exception => Failure(e)
    }
  }


  override def write(entityname: EntityName, df: DataFrame, mode: SaveMode, params: Map[String, String])(implicit ac: AdamContext): Try[Void] = {
    try {
      val entity = Entity.load(entityname).get
      val schema = entity.schema().filterNot(_.pk)
        .filter(attribute => attribute.storagehandler.isDefined && attribute.storagehandler.get.isInstanceOf[SolrHandler])
        .map(attribute => CatalogOperator.getAttributeOption(entityname, name, "solrfieldname") -> attribute)

      df.foreachPartition(pit => {
        val partClient = new HttpSolrClient(url + "/" + entityname.toString)

        pit.foreach { row => {
          val doc = new SolrInputDocument()
          doc.addField("id", row.getAs[Any](entity.pk.name))

          schema.foreach { case (name, attribute) => {
            //by appending _txt to the name of the attribute, solr will treat the field as a text field which is
            //indexed and tokenized; furthermore this allows us to dynamically add fields!
            //TODO: extend the suffixes based on the fieldtype
            val name = CatalogOperator.getAttributeOption(entityname, attribute.name, "solrfieldname")
            doc.addField(name.getOrElse(attribute.name), row.getAs[String](attribute.name))
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
