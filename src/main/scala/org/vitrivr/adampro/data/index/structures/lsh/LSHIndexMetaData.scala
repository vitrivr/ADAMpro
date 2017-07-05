package org.vitrivr.adampro.data.index.structures.lsh

import org.vitrivr.adampro.data.index.structures.lsh.hashfunction.Hasher
import org.vitrivr.adampro.query.distance.DistanceFunction

/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
private[lsh]
case class LSHIndexMetaData(ghashf : Array[Hasher], radius : Float, distance : DistanceFunction, m : Int) extends Serializable