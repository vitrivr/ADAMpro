package org.vitrivr.adampro.index.structures.lsh.hashfunction

import org.vitrivr.adampro.datatypes.feature.Feature.FeatureVector

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
trait LSHashFunction extends Serializable {
  def hash(vector: FeatureVector): Int
}
