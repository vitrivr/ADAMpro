package ch.unibas.dmi.dbis.adam.index.structures

/**
 * adamtwo
 *
 * Ivan Giangreco
 * October 2015
 */
object IndexStructures {
  sealed abstract class IndexStructureType(val name : String)

  case object ECP extends IndexStructureType("ecp")
  case object LSH extends IndexStructureType("lsh")
  case object SH extends IndexStructureType("sh")
  case object VAF extends IndexStructureType("vaf")
  case object VAV extends IndexStructureType("vav")

  def values : Seq[IndexStructureType] = Seq(ECP, LSH, SH, VAF, VAV)

  def withName(name : String) : IndexStructureType = values.find(_.name == name).getOrElse(null)
}