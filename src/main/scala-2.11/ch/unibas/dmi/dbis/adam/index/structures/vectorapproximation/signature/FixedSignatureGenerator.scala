package ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.signature

import java.util.BitSet

import ch.unibas.dmi.dbis.adam.datatypes.bitString.BitString

import scala.collection.mutable.ListBuffer

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
class FixedSignatureGenerator(val numberOfDimensions: Int, val numberOfBitsPerDimension: Int) extends SignatureGenerator with Serializable {

  /**
   *
   * @param cells
   * @return
   */
  def toSignature(cells: Seq[Int]): BitString[_] = {
    //require(cells.forall { cell => numberOfBitsPerDimension <= math.max(1.0, math.ceil(math.log(cell) / math.log(2))) })
    //require(cells.size == numberOfDimensions)

    val setBits = ListBuffer[Int]()

    cells.reverse.zipWithIndex.foreach {
      case (cell, dimIdx) =>
        val cellBits = BitSet.valueOf(Array(cell.toLong))

        var bitPosition = - 1
        var fromPosition = 0
        do{
            bitPosition = cellBits.nextSetBit(fromPosition)
        if(bitPosition != -1){
          fromPosition = bitPosition + 1
          setBits.+=(bitPosition + numberOfBitsPerDimension * dimIdx)
        }
        } while(bitPosition != -1)
    }

    BitString.fromBitIndicesToSet(setBits)
  }

  /**
   *
   * @param signature
   * @return
   */
  @inline def toCells(signature: BitString[_]): Seq[Int] = {
    val res = new Array[Int](numberOfDimensions)

    var dimIdx = 0
    while(dimIdx < numberOfDimensions){
      res(numberOfDimensions - 1 - dimIdx) = signature.get(dimIdx * numberOfBitsPerDimension, (dimIdx + 1) * numberOfBitsPerDimension)
      dimIdx += 1
    }

    res
  }
}