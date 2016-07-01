package ch.unibas.dmi.dbis.adam.helpers.repartition

/**
  * adampar
  *
  * Created by silvan on 20.06.16.
  */
object PartitionerChoice extends Enumeration {
  val SPARK = Value("Let spark handle repartitioning")
  val RANDOM = Value("Random Partitioning")
  val CURRENT = Value("Current implementation")
}
