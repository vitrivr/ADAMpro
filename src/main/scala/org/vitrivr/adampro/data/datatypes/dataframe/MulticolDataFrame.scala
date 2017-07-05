package org.vitrivr.adampro.data.datatypes.dataframe

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * January 2017
  *
  * from: http://stackoverflow.com/questions/32196207/derive-multiple-columns-from-a-single-column-in-a-spark-dataframe
  */
object MulticolDataFrame {

  implicit class MulticolDataFrame(df: DataFrame) {

    def flattenColumn(col: String) = {
      def addColumns(df: DataFrame, cols: Array[String]): DataFrame = {
        if (cols.isEmpty) df
        else addColumns(
          df.withColumn(col + "_" + cols.head, df(col + "." + cols.head)),
          cols.tail
        )
      }

      val field = df.select(col).schema.fields(0)
      val newCols = field.dataType.asInstanceOf[StructType].fields.map(x => x.name)

      addColumns(df, newCols).drop(col)
    }

    def withColumnMany(colName: String, col: Column) = {
      df.withColumn(colName, col).flattenColumn(colName)
    }

  }
}
