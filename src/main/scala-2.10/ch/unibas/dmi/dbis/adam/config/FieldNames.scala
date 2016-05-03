package ch.unibas.dmi.dbis.adam.config

/**
  * adamtwo
  *
  * Ivan Giangreco
  * March 2016
  */
object FieldNames {
  val idColumnName = "adamproid"
  val distanceColumnName = "adamprodistance"

  val featureIndexColumnName = "adamproindexfeature"

  val reservedNames = Seq(idColumnName, distanceColumnName, featureIndexColumnName)
}
