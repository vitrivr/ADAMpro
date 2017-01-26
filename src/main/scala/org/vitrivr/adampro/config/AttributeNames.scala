package org.vitrivr.adampro.config

/**
  * adamtwo
  *
  * Ivan Giangreco
  * March 2016
  */
object AttributeNames {

  val internalIdColumnName = "ap_id"

  val distanceColumnName = "ap_distance"
  val scoreColumnName = "ap_score"

  val indexableColumnName = "ap_indexable"
  val featureIndexColumnName = "ap_indexfeature"

  val partitionColumnName = "ap_partition"
  val sourceColumnName = "ap_source"

  val partitionKey = "ap_partitionkey"

  val reservedNames = Seq(internalIdColumnName, distanceColumnName, scoreColumnName, indexableColumnName, featureIndexColumnName, partitionColumnName, sourceColumnName, partitionKey)

  /**
    *
    * @param name
    * @return
    */
  def isNameReserved(name : String) = {
    if(name.startsWith("ap_")){
      true
    } else if(reservedNames.contains(name)){
      true
    } else{
      false
    }
  }
}
