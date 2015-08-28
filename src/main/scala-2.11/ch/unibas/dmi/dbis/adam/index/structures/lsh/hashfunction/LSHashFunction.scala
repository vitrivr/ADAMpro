package ch.unibas.dmi.dbis.adam.index.structures.lsh.hashfunction

import ch.unibas.dmi.dbis.adam.data.types.Feature.WorkingVector

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
  def hash(vector: WorkingVector): Int;
}
