package ch.unibas.dmi.dbis.adam.config

/**
  * adamtwo
  *
  * Ivan Giangreco
  * March 2016
  */
object FieldNames {
  val idColumnName = "adamproid"
  val internFeatureColumnName = "adamprofeature"
  val distanceColumnName = "adamprodistance"

  val featureColumnName = "feature"

  val featureIndexColumnName = "adamproindexfeature"

  val reservedNames = Seq(idColumnName, internFeatureColumnName, distanceColumnName, featureColumnName, featureIndexColumnName)
}
