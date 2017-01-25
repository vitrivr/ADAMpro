package org.vitrivr.adampro.query

import org.vitrivr.adampro.config.FieldNames
import org.vitrivr.adampro.datatypes.TupleID
import org.vitrivr.adampro.entity.AttributeDefinition
import org.vitrivr.adampro.query.distance.Distance
import org.vitrivr.adampro.query.distance.Distance.Distance
import org.apache.spark.sql.types.{FloatType, StructField, StructType}
import org.vitrivr.adampro.query.distance.Distance.Distance

/**
  * adamtwo
  *
  * Ivan Giangreco
  * August 2015
  */
object Result {
  def resultSchema = StructType(Seq(
    StructField(FieldNames.internalIdColumnName, TupleID.SparkTupleID, nullable = true),
    StructField(FieldNames.distanceColumnName, Distance.SparkDistance, nullable = true)
  ))
}

