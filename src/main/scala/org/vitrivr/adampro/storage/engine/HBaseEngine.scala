package org.vitrivr.adampro.storage.engine

import java.io.Serializable

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.NamespaceDescriptor
import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.execution.datasources.hbase.{HBaseAdapter, HBaseTableCatalog}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.vitrivr.adampro.data.datatypes.AttributeTypes
import org.vitrivr.adampro.data.datatypes.AttributeTypes.{AUTOTYPE, AttributeType, BOOLEANTYPE, DOUBLETYPE, FLOATTYPE, INTTYPE, LONGTYPE, STRINGTYPE, VECTORTYPE}
import org.vitrivr.adampro.data.entity.AttributeDefinition
import org.vitrivr.adampro.process.SharedComponentContext
import org.vitrivr.adampro.query.query.Predicate
import org.vitrivr.adampro.utils.exception.GeneralAdamException

import scala.util.{Failure, Success, Try}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * August 2017
  */
@Experimental class HBaseEngine()(@transient override implicit val ac: SharedComponentContext) extends Engine()(ac) with Serializable {
  override val name = "hbase"

  override def supports = Seq(AttributeTypes.AUTOTYPE, AttributeTypes.INTTYPE, AttributeTypes.LONGTYPE, AttributeTypes.FLOATTYPE, AttributeTypes.DOUBLETYPE,  AttributeTypes.STRINGTYPE, AttributeTypes.VECTORTYPE)

  override def specializes = Seq(AttributeTypes.VECTORTYPE)

  override val repartitionable = true

  private val namespace = "default"
  private val adapter = new HBaseAdapter(ac.sqlContext)

  import org.apache.hadoop.conf.Configuration
  import org.apache.hadoop.hbase.HBaseConfiguration

  val hbaseConf: Configuration = HBaseConfiguration.create()

  /**
    *
    * @param props
    */
  def this(props: Map[String, String])(implicit ac: SharedComponentContext) {
    this()(ac)
  }

  /**
    *
    * @param storename
    * @param attributes
    * @return
    */
  private def hbaseCatalog(storename : String, attributes : Seq[AttributeDefinition]) : String = {
    val pk = attributes.filter(_.pk).head

    val attrCatalog = attributes
      .map(a => s""""${a.name}":{"cf":"data", "col":"${a.name}", ${getHBaseType(a.attributeType)}}""".stripMargin).mkString(",")


    s"""{
       |"table":{"namespace":"$namespace", "name":"$storename"},
       |"rowkey":"${pk.name}",
       |"columns":{
       |"${pk.name}":{"cf":"rowkey", "col":"${pk.name}", "type":"${getHBaseType(pk.attributeType)}"},
       |$attrCatalog
       |}
       |}""".stripMargin
  }

  /**
    *
    * @param attributetype
    * @return
    */
  private def getHBaseType(attributetype: AttributeType): String = attributetype match {
    case AUTOTYPE => s""""type":""bigint""""
    case INTTYPE => s""""type":""int""""
    case LONGTYPE => s""""type":""bigint""""
    case FLOATTYPE => s""""type":""float""""
    case DOUBLETYPE => s""""type":""double""""
    case BOOLEANTYPE => s""""type":""boolean""""
    case STRINGTYPE => s""""type":""string""""
    case VECTORTYPE => s""""avro":""schema_vector""""
    case _ => throw new GeneralAdamException("attribute type " + attributetype.name + " is not supported in HBase handler")
  }

  val schema_vector = s"""{"type": "array", "items": ["float"]}""".stripMargin


  /**
    * Create the entity.
    *
    * @param storename  adapted entityname to store feature to
    * @param attributes attributes of the entity (w.r.t. handler)
    * @param params     creation parameters
    * @return options to store
    */
  override def create(storename: String, attributes: Seq[AttributeDefinition], params: Map[String, String])(implicit ac: SharedComponentContext): Try[Map[String, String]] = {
    val schema = StructType(attributes.map(attribute => StructField(attribute.name.toString, attribute.attributeType.datatype)))
    val df = ac.spark.createDataFrame(ac.sc.emptyRDD[Row], schema)

    try{
      df.write
        .mode(SaveMode.ErrorIfExists)
        .options(Map("schema_vector"->schema_vector, HBaseTableCatalog.tableCatalog -> hbaseCatalog(storename, attributes), HBaseTableCatalog.tableCatalog -> storename))
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .save()

      Success(Map())
    } catch {
      case e: Exception => Failure(e)
    }
  }

  /**
    * Check if entity exists.
    *
    * @param storename adapted entityname to store feature to
    * @return
    */
  override def exists(storename: String)(implicit ac: SharedComponentContext): Try[Boolean] = {
    try{
      Success(adapter.admin.isTableAvailable(TableName.valueOf(namespace, storename)))
    } catch {
      case e: Exception => Failure(e)
    }
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
      val res = ac.sqlContext
        .read
        .options(Map("schema_vector"->schema_vector, HBaseTableCatalog.tableCatalog -> hbaseCatalog(storename, attributes)))
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .load()

      Success(res)
    } catch {
      case e: Exception => Failure(e)
    }
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
  override def write(storename: String, df: DataFrame, attributes: Seq[AttributeDefinition], mode: SaveMode, params: Map[String, String])(implicit ac: SharedComponentContext): Try[Map[String, String]] = {
    try {
      df.write
        .mode(mode)
        .options(Map("schema_vector"->schema_vector, HBaseTableCatalog.tableCatalog -> storename))
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .save()

      Success(null)
    } catch {
      case e: Exception => Failure(e)
    }
  }

  /**
    * Drop the entity.
    *
    * @param storename adapted entityname to store feature to
    * @return
    */
  override def drop(storename: String)(implicit ac: SharedComponentContext): Try[Void] = {
    try {
      adapter.admin.deleteTable(TableName.valueOf(namespace, storename))
      Success(null)
    } catch {
      case e: Exception => Failure(e)
    }
  }
}