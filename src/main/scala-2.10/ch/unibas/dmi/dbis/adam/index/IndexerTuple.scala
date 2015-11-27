package ch.unibas.dmi.dbis.adam.index

import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature.FeatureVector
import ch.unibas.dmi.dbis.adam.entity.Tuple._

/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
case class IndexerTuple(tid: TupleID, value: FeatureVector)