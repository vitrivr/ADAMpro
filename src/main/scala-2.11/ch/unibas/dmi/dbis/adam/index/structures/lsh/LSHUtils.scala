package ch.unibas.dmi.dbis.adam.index.structures.lsh

import ch.unibas.dmi.dbis.adam.data.types.Feature._
import ch.unibas.dmi.dbis.adam.data.types.bitString.BitString

/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
private[lsh] object LSHUtils {
  /**
   *
   * @param f
   * @return
   */
  @inline def hashFeature(f : WorkingVector, indexMetaData: LSHIndexMetaData) : BitString[_] = {
    val indices = indexMetaData.hashTables.map(ht => ht(f))
    BitString.fromBitIndicesToSet(indices)
  }
}
