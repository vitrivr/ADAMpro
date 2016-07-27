package ch.unibas.dmi.dbis.adam.config

/**
  * adamtwo
  *
  * Ivan Giangreco
  * March 2016
  */
object FieldNames {
  val distanceColumnName = "ap_distance"
  val featureIndexColumnName = "ap_indexfeature"

  val partitionColumnName = "ap_partition"

  val reservedNames = Seq(distanceColumnName, featureIndexColumnName, partitionColumnName)
}
