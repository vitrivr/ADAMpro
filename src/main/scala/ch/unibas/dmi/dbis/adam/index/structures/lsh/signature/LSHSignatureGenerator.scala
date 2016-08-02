package ch.unibas.dmi.dbis.adam.index.structures.lsh.signature

import ch.unibas.dmi.dbis.adam.datatypes.bitString.BitString
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.index.structures.lsh.hashfunction.Hasher
import ch.unibas.dmi.dbis.adam.index.structures.va.signature.FixedSignatureGenerator

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
