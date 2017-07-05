package org.vitrivr.adampro.data.index.structures.ecp

import org.vitrivr.adampro.data.datatypes.TupleID._
import org.vitrivr.adampro.data.datatypes.vector.Vector._
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