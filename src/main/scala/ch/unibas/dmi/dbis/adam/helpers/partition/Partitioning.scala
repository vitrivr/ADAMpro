package ch.unibas.dmi.dbis.adam.helpers.partition

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * July 2016
  */
object Partitioning extends Serializable{
  type PartitionID = Int
}

object PartitionMode extends Enumeration with Serializable{
  val CREATE_NEW = Value("create new data (materialize)")
  val REPLACE_EXISTING = Value("replace existing data (materialize)")
  val CREATE_TEMP = Value("create temporary data in cache")
}

object PartitionerChoice extends Enumeration with Serializable{
  val SPARK = Value("Let spark handle repartitioning")
  val RANDOM = Value("Random Partitioning")
  val ECP = Value("Partition via ECP")
}
