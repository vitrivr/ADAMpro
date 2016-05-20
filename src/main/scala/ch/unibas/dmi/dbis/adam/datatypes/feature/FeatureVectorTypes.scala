package ch.unibas.dmi.dbis.adam.datatypes.feature

/**
 * adamtwo
 *
 * Ivan Giangreco
 * October 2015
 */
object FeatureVectorTypes {
  sealed abstract class FeatureVectorType(val num : Byte)

  case object DenseFeatureVectorType extends FeatureVectorType(0)
  case object SparseFeatureVectorType extends FeatureVectorType(1)
  case object IntVectorType extends FeatureVectorType(2)
}

