package org.vitrivr.adampro.index.structures.ecp

import org.vitrivr.adampro.datatypes.TupleID._
import org.vitrivr.adampro.datatypes.vector.Vector._
import org.vitrivr.adampro.query.distance.DistanceFunction


/**
 * adamtwo
 *
 * Ivan Giangreco
 * October 2015
 */
@SerialVersionUID(100L)
case class ECPIndexMetaData(leaders : Seq[ECPLeader], distance : DistanceFunction) extends Serializable

case class ECPLeader(id: TupleID, vector: DenseMathVector, count : Long) extends Serializable