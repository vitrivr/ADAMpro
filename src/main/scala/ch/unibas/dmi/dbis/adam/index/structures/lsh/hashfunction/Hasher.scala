package ch.unibas.dmi.dbis.adam.index.structures.lsh.hashfunction

import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature.FeatureVector

/**
  * adamtwo
  *
  * Ivan Giangreco
  * August 2015
  */
@SerialVersionUID(100L)
class Hasher(private val functions: Array[LSHashFunction]) extends Serializable {
  //possibly related to http://stackoverflow.com/questions/16386252/scala-deserialization-class-not-found
  //here we have to use an array, rather than a Seq or a List!

  /**
    *
    * @param family  family of hash functions
    * @param nHashes number of hashes
    */
  def this(family: () => LSHashFunction, nHashes: Int) {
    this((0 until nHashes).map(x => family()).toArray)
  }

  /**
    *
    * @param v feature vector to hash
    * @return
    */
  def apply(v: FeatureVector, m: Int = 0): Int = {
    if (m == 0) {
      functions.map(f => f.hash(v)).hashCode()
    } else {
      functions.map(f => f.hash(v)).hashCode() % m
    }
  }
}
