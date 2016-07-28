package ch.unibas.dmi.dbis.adam.config

/**
  * adamtwo
  *
  * Ivan Giangreco
  * March 2016
  */
object FieldNames {
  val pk = "id"   //TODO ap_pk and add to reservednames

  val distanceColumnName = "ap_distance"
  val featureIndexColumnName = "ap_indexfeature"

  val partitionColumnName = "ap_partition"
  val sourceColumnName = "ap_source"

  val partitionKey = "ap_partitionkey"

  val reservedNames = Seq(distanceColumnName, featureIndexColumnName, partitionColumnName, sourceColumnName, partitionKey)
}
