package ch.unibas.dmi.dbis.adam.index.structures.va.signature

import ch.unibas.dmi.dbis.adam.datatypes.bitString.BitString

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
private[va] trait SignatureGenerator extends Serializable {
  /**
   * 
   */
  def toSignature(cells: Seq[Int]): BitString[_]
  
  /**
   * 
   */
  def toCells(signature: BitString[_]): Seq[Int]
}
