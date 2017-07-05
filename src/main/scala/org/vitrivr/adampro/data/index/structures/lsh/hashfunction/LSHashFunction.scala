package org.vitrivr.adampro.data.index.structures.lsh.hashfunction

import org.vitrivr.adampro.data.datatypes.vector.Vector._

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
trait LSHashFunction extends Serializable {
  def hash(vector: MathVector): Int
}
