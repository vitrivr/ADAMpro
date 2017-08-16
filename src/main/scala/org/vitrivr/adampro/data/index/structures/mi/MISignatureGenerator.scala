package org.vitrivr.adampro.data.index.structures.mi

import org.vitrivr.adampro.data.datatypes.TupleID.TupleID
import org.vitrivr.adampro.data.datatypes.bitstring.BitString
import org.vitrivr.adampro.data.index.structures.va.signature.FixedSignatureGenerator

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
  def toSignature(f: Seq[Int]) : BitString[_] = {
    signatureGenerator.toSignature(f)
  }

  /**
    *
    * @param b
    * @return
    */
  def toBuckets(b : BitString[_]) : IndexedSeq[Int] = {
    signatureGenerator.toCells(b)
  }
}
