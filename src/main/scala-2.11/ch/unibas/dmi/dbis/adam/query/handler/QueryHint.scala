package ch.unibas.dmi.dbis.adam.query.handler

import ch.unibas.dmi.dbis.adam.index.Index.IndexTypeName
import ch.unibas.dmi.dbis.adam.index.structures.IndexStructures._

/**
 * adamtwo
 *
 * Ivan Giangreco
 * November 2015
 */
object QueryHints {
  sealed abstract class QueryHint
  sealed abstract class SimpleQueryHint extends QueryHint
  sealed abstract class IndexQueryHint(val structureType : IndexTypeName) extends SimpleQueryHint
  sealed abstract class CompoundQueryHint(val hints : Seq[SimpleQueryHint]) extends QueryHint

  case object SEQUENTIAL_QUERY extends SimpleQueryHint
  case object INDEX_QUERY extends CompoundQueryHint(Seq(VAF_INDEX_QUERY, VAV_INDEX_QUERY, SH_INDEX_QUERY, ECP_INDEX_QUERY, LSH_INDEX_QUERY))
  case object INEXACT_QUERY extends CompoundQueryHint(Seq(SH_INDEX_QUERY, ECP_INDEX_QUERY, LSH_INDEX_QUERY))
  case object ECP_INDEX_QUERY extends IndexQueryHint(ECP)
  case object LSH_INDEX_QUERY extends IndexQueryHint(LSH)
  case object SH_INDEX_QUERY extends IndexQueryHint(SH)
  case object EXACT_QUERY extends CompoundQueryHint(Seq(VAF_INDEX_QUERY, VAV_INDEX_QUERY, SEQUENTIAL_QUERY))
  case object VA_INDEX_QUERY extends CompoundQueryHint(Seq(VAF_INDEX_QUERY, VAV_INDEX_QUERY))
  case object VAF_INDEX_QUERY extends IndexQueryHint(VAF)
  case object VAV_INDEX_QUERY extends IndexQueryHint(VAV)

  val FALLBACK_HINTS : QueryHint = EXACT_QUERY

  def withName(s : String) : Option[QueryHint] = s match {
    case "sequential" => Option(SEQUENTIAL_QUERY)
    case "index" => Option(INDEX_QUERY)
    case "inexact" => Option(INEXACT_QUERY)
    case "ecp" => Option(ECP_INDEX_QUERY)
    case "lsh" => Option(LSH_INDEX_QUERY)
    case "sh" => Option(SH_INDEX_QUERY)
    case "exact" => Option(EXACT_QUERY)
    case "va" => Option(VA_INDEX_QUERY)
    case "vaf" => Option(VAF_INDEX_QUERY)
    case "vav" => Option(VAV_INDEX_QUERY)
    case _ => None
  }
}
