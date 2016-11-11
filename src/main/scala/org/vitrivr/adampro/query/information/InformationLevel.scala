package org.vitrivr.adampro.query.information

/**
  * adamtwo
  *
  * Ivan Giangreco
  * May 2016
  */
object InformationLevels {

  sealed abstract class InformationLevel(val name: String) extends Serializable {
    def equals(other: InformationLevel): Boolean = other.name.equals(name)
  }

  case object FULL_TREE extends InformationLevel("full tree")
  case object LAST_STEP_ONLY extends InformationLevel("only last results")
  case object INTERMEDIATE_RESULTS extends InformationLevel("with intermediate results")
  case object PARTITION_PROVENANCE extends InformationLevel("partition provenance")
  case object SOURCE_PROVENANCE extends InformationLevel("source provenance")
}