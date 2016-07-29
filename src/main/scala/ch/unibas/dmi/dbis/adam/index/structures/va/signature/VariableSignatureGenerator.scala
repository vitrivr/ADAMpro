package ch.unibas.dmi.dbis.adam.index.structures.va.signature

import java.util.BitSet

import ch.unibas.dmi.dbis.adam.datatypes.bitString.BitString

import scala.collection.mutable.ListBuffer

/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
class VariableSignatureGenerator (numberOfBitsPerDimension: Array[Int]) extends SignatureGenerator with Serializable {
  var parNumberOfBitsPerDimension = numberOfBitsPerDimension.par //allows huge speedup!
  val numberOfDimensions: Int = numberOfBitsPerDimension.length


  /**
   *
    * @param cells cell ids to translate to signature
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


    BitString(setBits)
  }

  /**
   *
    * @param signature signature to translate to cell ids
   * @return
   */
  @inline def toCells(signature: BitString[_]): Seq[Int] = {
    val lengths = numberOfBitsPerDimension
    assert(lengths.count(_ > 32) < 1)

    val indexes = signature.getBitIndexes
    var i = 0

    val bitIntegers = new Array[Int](lengths.length)
    var dim = 1

    var sum = 0

    while (i < indexes.length) {
      val index = indexes(i)

      while (index >= sum + lengths(lengths.length - dim)) {
        sum += lengths(lengths.length - dim)
        dim += 1
      }

      bitIntegers(lengths.length - dim) |= (1 << (index - sum))

      i += 1
    }

    bitIntegers
  }
}
