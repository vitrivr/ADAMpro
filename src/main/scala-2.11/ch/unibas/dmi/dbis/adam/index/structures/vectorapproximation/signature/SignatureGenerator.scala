package ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.signature

import ch.unibas.dmi.dbis.adam.data.types.bitString.BitString

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
private[vectorapproximation] trait SignatureGenerator extends Serializable {
  /**
   * 
   */
  def toSignature(cells: Seq[Int]): BitString[_]
  
  /**
   * 
   */
  def toCells(signature: BitString[_]): Seq[Int]
}
