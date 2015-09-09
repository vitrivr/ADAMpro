package ch.unibas.dmi.dbis.adam.index.structures.lsh

import ch.unibas.dmi.dbis.adam.datatypes.Feature
import Feature._
import ch.unibas.dmi.dbis.adam.datatypes.bitString.BitString

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
