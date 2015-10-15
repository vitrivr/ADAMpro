package ch.unibas.dmi.dbis.adam.index.structures.ecp

import ch.unibas.dmi.dbis.adam.datatypes.Feature.WorkingVector
import ch.unibas.dmi.dbis.adam.index.IndexerTuple
import ch.unibas.dmi.dbis.adam.query.distance.NormBasedDistanceFunction


/**
 * adamtwo
 *
 * Ivan Giangreco
 * October 2015
 */
/**
 *
 * @param leaders
 * @param distance
 */
private[ecp] case class ECPIndexMetaData(leaders : Array[IndexerTuple[WorkingVector]], distance : NormBasedDistanceFunction) {}
