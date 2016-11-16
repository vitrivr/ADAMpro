package org.vitrivr.adampro.utils

import org.vitrivr.adampro.api.EntityOp
import org.vitrivr.adampro.datatypes.FieldTypes
import org.vitrivr.adampro.entity.AttributeDefinition
import org.vitrivr.adampro.main.{AdamContext, SparkStartup}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.reflect.io.File

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * July 2016
  */
class SolrImporter(file : File)(implicit ac: AdamContext) {
  def apply(): Unit ={
    val source = scala.io.Source.fromFile(file.toAbsolute.path)

    val data = source.getLines.toSeq.map{
      line =>
        val splits = line.split("\t")
        val id = splits(0).toLong
        val text = splits(1)

        Row(id, text)
    }

    val statusCreate = EntityOp.create("features_fulltext", Seq(new AttributeDefinition("id", FieldTypes.LONGTYPE, true), new AttributeDefinition("text", FieldTypes.TEXTTYPE, false)))

    if(statusCreate.isFailure){
      throw statusCreate.failed.get
    }

    val dfSchema = StructType(Seq(StructField("id", LongType), StructField("text", StringType)))
    var df = ac.sqlContext.createDataFrame(ac.sc.parallelize(data), dfSchema)

    val exists = EntityOp.exists("features_fulltext")

    val statusInsert = EntityOp.insert("features_fulltext", df)

    if(statusInsert.isFailure){
      throw statusInsert.failed.get
    }
  }
}

object SolrImporter {
  def apply(path : String)(implicit ac: AdamContext): Unit = {
    new SolrImporter(File(path))(ac)()
  }

  def main(args: Array[String]): Unit = {
    //for experimental reasons only
    val ac = SparkStartup.mainContext

    apply(args(0))(ac)
  }
}
