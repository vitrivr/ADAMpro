package ch.unibas.dmi.dbis.adam.index.structures.lsh.hashfunction

import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature.FeatureVector

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
sealed class Hasher(val functions: List[LSHashFunction]) extends Serializable {
  /**
   *
   * @param family
   * @param nHashes
   */
  def this(family: () => LSHashFunction, nHashes: Int) {
    this((0 until nHashes).map(x => family()).toList)
  }

  /**
   *
   * @param v
   * @return
   */
  def apply(v: FeatureVector, m : Int = 0): Int = {
    if ( m == 0){
      functions.map(f => f.hash(v)).hashCode()
    } else {
      functions.map(f => f.hash(v)).hashCode() % m
    }
  }
}
