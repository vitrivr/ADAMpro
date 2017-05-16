package org.vitrivr.adampro.index

import org.vitrivr.adampro.datatypes.TupleID.TupleID
import org.vitrivr.adampro.index.structures.mi.MIResultElement
import org.vitrivr.adampro.query.distance.Distance.Distance

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * January 2017
  */
case class ResultElement(ap_id: TupleID, ap_lower : Distance, ap_upper : Distance, ap_distance : Distance) extends Serializable with Ordered[ResultElement] {
  def compare(that: ResultElement): Int = this.ap_distance.compare(that.ap_distance)
}

