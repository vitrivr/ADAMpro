package ch.unibas.dmi.dbis.adam.index

import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature.FeatureVector

/**
  * adamtwo
  *
  * Tuple containing all data necessary at the indexing task (i.e., when the index is created).
  *
  * Ivan Giangreco
  * September 2015
  */
@SerialVersionUID(100L)
case class IndexingTaskTuple[A](id: A, feature: FeatureVector) extends Serializable