package ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.signature

import ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.VectorApproximationIndexer.Signature

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
  def toSignature(cells: Seq[Int]): Signature
  
  /**
   * 
   */
  def toCells(signature: Signature): Seq[Int]
}
