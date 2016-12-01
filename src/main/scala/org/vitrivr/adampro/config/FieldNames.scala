package org.vitrivr.adampro.config

/**
  * adamtwo
  *
  * Ivan Giangreco
  * March 2016
  */
object FieldNames {

  val internalIdColumnName = "ap_id"

  val distanceColumnName = "ap_distance"
  val scoreColumnName = "ap_score"

  val featureIndexColumnName = "ap_indexfeature"

  val partitionColumnName = "ap_partition"
  val sourceColumnName = "ap_source"

  val partitionKey = "ap_partitionkey"

  val reservedNames = Seq(internalIdColumnName, distanceColumnName, scoreColumnName, featureIndexColumnName, partitionColumnName, sourceColumnName, partitionKey)
}
