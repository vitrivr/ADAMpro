package org.vitrivr.adampro.index.structures.lsh.signature

import org.vitrivr.adampro.datatypes.bitString.BitString
import org.vitrivr.adampro.datatypes.feature.Feature._
import org.vitrivr.adampro.index.structures.lsh.hashfunction.Hasher
import org.vitrivr.adampro.index.structures.va.signature.FixedSignatureGenerator

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * August 2016
  */
class LSHSignatureGenerator(hashTables : Seq[Hasher], m : Int) extends Serializable {
  val signatureGenerator = new FixedSignatureGenerator(hashTables.length, math.ceil(math.log(m) / math.log(2)).toInt)

  /**
    *
    * @param f
    * @return
    */
  def toSignature(f: FeatureVector) : BitString[_] = {
    signatureGenerator.toSignature(toBuckets(f))
  }

  /**
    *
    * @param f
    * @return
    */
  def toBuckets(f: FeatureVector) = {
    hashTables.map(ht => ht(f,m))
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
