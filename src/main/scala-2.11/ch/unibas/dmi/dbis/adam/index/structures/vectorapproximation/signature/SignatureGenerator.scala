package ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.signature

import ch.unibas.dmi.dbis.adam.data.types.bitString.BitString._

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
trait SignatureGenerator extends Serializable {
  /**
   * 
   */
  def toSignature(cells: Seq[Int]): BitStringType
  
  /**
   * 
   */
  def toCells(signature: BitStringType): Seq[Int]
}
