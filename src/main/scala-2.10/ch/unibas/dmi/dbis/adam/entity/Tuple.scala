package ch.unibas.dmi.dbis.adam.entity

import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature.FeatureVector

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
case class Tuple[A](val tid: A, val feature: FeatureVector)