package org.vitrivr.adampro.index.structures.va

import org.vitrivr.adampro.datatypes.TupleID.TupleID
import org.vitrivr.adampro.query.distance.Distance.Distance

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * May 2017
  */
case class VAResultElement(ap_id: TupleID, ap_lower : Distance, ap_upper : Distance, ap_distance : Distance) extends Serializable with Ordered[VAResultElement] {
  def compare(that: VAResultElement): Int = this.ap_distance.compare(that.ap_distance)
}
