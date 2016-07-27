package ch.unibas.dmi.dbis.adam.helpers.provenance

import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.main.AdamContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, Row}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * July 2016
  */
object ProvenanceInfoAppender {
  def apply(df: DataFrame, source: String, options: Map[String, String] = Map("provenanceInfo-provenance" -> "true", "provenanceInfo-partition" -> "true"))(implicit ac: AdamContext): DataFrame = {
    var result = df

    if (options.getOrElse("provenanceInfo-provenance", "false").equals("true")) {
      if (df.schema.fieldNames.contains(FieldNames.provenanceColumnName)) {
        val sourceUDF = udf((s: String) => s + " -> " + source)
        result = result.withColumn(FieldNames.provenanceColumnName, sourceUDF(col(FieldNames.provenanceColumnName)))
      } else {
        result = result.withColumn(FieldNames.provenanceColumnName, lit(source))
      }
    }


    if (options.getOrElse("provenanceInfo-partition", "false").equals("true")) {
      val rdd = result.rdd.mapPartitionsWithIndex((idx, iter) => iter.map(r => Row(r.toSeq ++ Seq(idx) : _*)), preservesPartitioning = true)
      result = ac.sqlContext.createDataFrame(rdd, result.schema.add(FieldNames.partitionColumnName, IntegerType))
    }

    result
  }
}
