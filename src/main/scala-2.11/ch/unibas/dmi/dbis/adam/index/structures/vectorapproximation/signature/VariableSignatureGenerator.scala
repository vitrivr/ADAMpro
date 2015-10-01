package ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.signature

import java.util.BitSet

import ch.unibas.dmi.dbis.adam.datatypes.bitString.BitString

import scala.collection.mutable.ListBuffer

/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
class VariableSignatureGenerator (val numberOfBitsPerDimension: Array[Int]) extends SignatureGenerator with Serializable {
  var parNumberOfBitsPerDimension = numberOfBitsPerDimension.par //allows huge speedup!
  val numberOfDimensions: Int = numberOfBitsPerDimension.length


  /**
   *
   * @param cells
   * @return
   */
  def toSignature(cells: Seq[Int]): BitString[_] = {
    val setBits = ListBuffer[Int]()

    var bitSum = 0
    var i = 0

    while(i < cells.length){
      val dimIdx = cells.length - 1 - i
      val cell = cells(dimIdx)
      val cellBits = BitSet.valueOf(Array(cell.toLong))

      var bitPosition = - 1
      var fromPosition = 0

      do{
        bitPosition = cellBits.nextSetBit(fromPosition)

        if(bitPosition != -1 && bitPosition < numberOfBitsPerDimension(dimIdx)){
           fromPosition = bitPosition + 1
           setBits.+=(bitPosition + bitSum)
        }
      } while(bitPosition != -1 && bitPosition < numberOfBitsPerDimension(dimIdx))

      bitSum += numberOfBitsPerDimension(dimIdx)
      i += 1
    }


    BitString.fromBitIndicesToSet(setBits)
  }

  /**
   *
   * @param signature
   * @return
   */
  @inline def toCells(signature: BitString[_]): Seq[Int] = {
    signature.getWithBitLengths(numberOfBitsPerDimension)
  }
}
