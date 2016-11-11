package org.vitrivr.adampro.query

import org.vitrivr.adampro.config.FieldNames
import org.vitrivr.adampro.entity.AttributeDefinition
import org.vitrivr.adampro.query.distance.Distance.Distance
import org.apache.spark.sql.types.{FloatType, StructField, StructType}

/**
  * adamtwo
  *
  * Ivan Giangreco
  * August 2015
  */
case class Result(distance: Distance, tid: Any) extends Ordered[Result] {
  override def compare(that: Result): Int = distance compare that.distance
}

/**
  *
  */
object Result {
  def resultSchema(pk : AttributeDefinition) = StructType(Seq(
    StructField(pk.name, pk.fieldtype.datatype, nullable = true),
    StructField(FieldNames.distanceColumnName, FloatType, nullable = true)
  ))
}

