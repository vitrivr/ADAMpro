package org.vitrivr.adampro.data.index.structures.va.signature

import org.vitrivr.adampro.data.datatypes.bitstring.BitString

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
@SerialVersionUID(100L)
private[va] trait SignatureGenerator extends Serializable {

  /**
    *
    * @param cells cell ids to translate to signature
    * @return
    */
  def toSignature(cells: Seq[Int]): BitString[_]

  /**
    *
    * @param signature signature to translate to cell ids
    * @return
    */
  def toCells(signature: BitString[_]): IndexedSeq[Int]
}
