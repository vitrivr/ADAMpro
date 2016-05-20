package ch.unibas.dmi.dbis.adam.storage.partition

/**
  * adamtwo
  *
  * Ivan Giangreco
  * May 2016
  */
object PartitionMode extends Enumeration {
  val CREATE_NEW = Value("create new data (materialize)")
  val REPLACE_EXISTING = Value("replace existing data (materialize)")
  val CREATE_TEMP = Value("create temporary data in cache")
}