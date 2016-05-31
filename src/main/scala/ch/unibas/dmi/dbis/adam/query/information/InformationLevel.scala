package ch.unibas.dmi.dbis.adam.query.information

/**
  * adamtwo
  *
  * Ivan Giangreco
  * May 2016
  */
object InformationLevels {

  sealed abstract class InformationLevel(val name: String) extends Serializable {
    def equals(other: InformationLevel): Boolean = (other.name.equals(name))
  }

  case object FULL_TREE_NO_INTERMEDIATE_RESULTS extends InformationLevel("full tree, no intermediate results")
  case object FULL_TREE_INTERMEDIATE_RESULTS extends InformationLevel("full tree with intermediate results")
  case object LAST_STEP_ONLY extends InformationLevel("only last results")

}