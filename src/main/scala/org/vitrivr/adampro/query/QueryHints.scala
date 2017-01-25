package org.vitrivr.adampro.query

import org.vitrivr.adampro.index.Index.IndexTypeName
import org.vitrivr.adampro.index.structures.IndexTypes._

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
  sealed abstract class ComplexQueryHint(val hints : Seq[SimpleQueryHint]) extends QueryHint

  case object SEQUENTIAL_QUERY extends SimpleQueryHint
  case object INDEX_QUERY extends ComplexQueryHint(Seq(VAF_INDEX_QUERY, VAV_INDEX_QUERY, PQ_INDEX_QUERY, ECP_INDEX_QUERY, SH_INDEX_QUERY, LSH_INDEX_QUERY))
  case object INEXACT_QUERY extends ComplexQueryHint(Seq(PQ_INDEX_QUERY, ECP_INDEX_QUERY, SH_INDEX_QUERY, LSH_INDEX_QUERY))
  case object ECP_INDEX_QUERY extends IndexQueryHint(ECPINDEX)
  case object MI_INDEX_QUERY extends IndexQueryHint(MIINDEX)
  case object LSH_INDEX_QUERY extends IndexQueryHint(LSHINDEX)
  case object PQ_INDEX_QUERY extends IndexQueryHint(PQINDEX)
  case object SH_INDEX_QUERY extends IndexQueryHint(SHINDEX)
  case object EXACT_QUERY extends ComplexQueryHint(Seq(VAF_INDEX_QUERY, VAV_INDEX_QUERY, SEQUENTIAL_QUERY))
  case object VA_INDEX_QUERY extends ComplexQueryHint(Seq(VAF_INDEX_QUERY, VAV_INDEX_QUERY))
  case object VAF_INDEX_QUERY extends IndexQueryHint(VAFINDEX)
  case object VAV_INDEX_QUERY extends IndexQueryHint(VAVINDEX)
  case object VAP_INDEX_QUERY extends IndexQueryHint(VAPLUSINDEX)
  case object EMPIRICAL extends SimpleQueryHint
  case object SCORED extends SimpleQueryHint

  val FALLBACK_HINTS : QueryHint = EXACT_QUERY

  def withName(s : Seq[String]) : Seq[QueryHint] = {
    if(s != null){
      s.map(withName(_)).filter(_.isDefined).map(_.get)
    } else {
      Seq()
    }
  }

  def withName(s : String) : Option[QueryHint] = s match {
    case "sequential" => Some(SEQUENTIAL_QUERY)
    case "index" => Some(INDEX_QUERY)
    case "inexact" => Some(INEXACT_QUERY)
    case "exact" => Some(EXACT_QUERY)
    case "vaf" => Some(VAF_INDEX_QUERY)
    case "predictive" => Some(EMPIRICAL)
    case "empirical" => Some(EMPIRICAL)
    case "scored" => Some(SCORED)
    case _ => None
  }
}
