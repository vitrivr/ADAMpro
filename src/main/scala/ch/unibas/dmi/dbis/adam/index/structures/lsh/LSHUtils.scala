package ch.unibas.dmi.dbis.adam.index.structures.lsh

import ch.unibas.dmi.dbis.adam.datatypes.bitString.BitString
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._

import scala.collection.mutable.ListBuffer

/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
private[lsh] object LSHUtils {
  /**
   *
   * @param f
   * @return
   */
  @inline def hashFeature(f: FeatureVector, indexMetaData: LSHIndexMetaData, m : Int = Int.MaxValue): BitString[_] = {
    val indices = indexMetaData.hashTables.map(ht => ht(f,m)).zipWithIndex.map{case(hash, idx) => int2Indices(hash, idx * math.ceil(math.log(m) / math.log(2)).toInt + 1)}.flatten
    BitString(indices)
  }

  private def int2Indices(hash: Int, zero : Int): Seq[Int] = {
    var value = hash
    val lb = ListBuffer[Int]()

    var index = 0

    while (value != 0L) {
      if (value % 2L != 0) {
       lb += index + zero
      }
      index += 1
      value = value >>> 1;
    }

    lb.toSeq
  }

}
