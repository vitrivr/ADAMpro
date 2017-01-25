package org.vitrivr.adampro.index.structures.lsh.hashfunction

import org.vitrivr.adampro.datatypes.vector.Vector._

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
trait LSHashFunction extends Serializable {
  def hash(vector: MathVector): Int
}
