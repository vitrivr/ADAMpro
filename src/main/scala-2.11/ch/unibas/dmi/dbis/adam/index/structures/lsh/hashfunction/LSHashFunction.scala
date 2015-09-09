package ch.unibas.dmi.dbis.adam.index.structures.lsh.hashfunction

import ch.unibas.dmi.dbis.adam.datatypes.Feature
import Feature.WorkingVector

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
trait LSHashFunction extends Serializable {

  /**
   *
   * @param vector
   * @return
   */
  def hash(vector: WorkingVector): Int
}
