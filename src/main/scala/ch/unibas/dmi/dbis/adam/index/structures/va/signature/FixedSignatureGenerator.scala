package ch.unibas.dmi.dbis.adam.index.structures.va.signature

import java.util.BitSet

import ch.unibas.dmi.dbis.adam.datatypes.bitString.BitString

import scala.collection.mutable.ListBuffer

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
class FixedSignatureGenerator(val dimensions: Int, val bitsPerDimension: Int) extends SignatureGenerator with Serializable {

  /**
   *
   * @param cells cell ids to translate to signature
   * @return
   */
  def toSignature(cells: Seq[Int]): BitString[_] = {
    val setBits = ListBuffer[Int]()

    cells.reverse.zipWithIndex.foreach {
      case (cell, dimIdx) =>
        val cellBits = BitSet.valueOf(Array(cell.toLong))

        var bitPosition = - 1
        var fromPosition = 0
        do{
          bitPosition = cellBits.nextSetBit(fromPosition)
          if(bitPosition != -1  && bitPosition < bitsPerDimension){
            fromPosition = bitPosition + 1
            setBits.+=(bitPosition + bitsPerDimension * dimIdx)
          }
        } while(bitPosition != -1 && bitPosition < bitsPerDimension)
    }

    BitString(setBits)
  }

  /**
   *
   * @param signature signature to translate to cell ids
   * @return
   */
  @inline def toCells(signature: BitString[_]): Seq[Int] = signature.toInts(dimensions, bitsPerDimension)

}