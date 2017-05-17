package org.vitrivr.adampro.index.structures.mi

import org.vitrivr.adampro.datatypes.TupleID.TupleID
import org.vitrivr.adampro.datatypes.bitstring.BitString
import org.vitrivr.adampro.index.structures.va.signature.FixedSignatureGenerator

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * May 2017
  */
class MISignatureGenerator(ki : Int, nrefs : Int) extends Serializable{
  val signatureGenerator = new FixedSignatureGenerator(ki, math.ceil(math.log(nrefs) / math.log(2)).toInt)

  /**
    *
    * @param f
    * @return
    */
  def toSignature(f: Seq[TupleID]) : BitString[_] = {
    signatureGenerator.toSignature(f.map(_.toInt))
  }

  /**
    *
    * @param b
    * @return
    */
  def toBuckets(b : BitString[_]) = {
    signatureGenerator.toCells(b)
  }
}
