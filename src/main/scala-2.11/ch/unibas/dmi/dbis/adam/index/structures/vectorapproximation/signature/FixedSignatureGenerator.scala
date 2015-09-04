package ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.signature

import java.util.BitSet

import ch.unibas.dmi.dbis.adam.data.types.bitString.BitString
import ch.unibas.dmi.dbis.adam.data.types.bitString.BitString._

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
   */
  def toSignature(cells: Seq[Int]): BitStringType = {
//    require(cells.forall { cell => numberOfBitsPerDimension <= math.max(1.0, math.ceil(math.log(cell) / math.log(2))) })
//    require(cells.size == numberOfDimensions)

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
   */
  def toCells(signature: BitStringType): Seq[Int] = {
    (0 until numberOfDimensions).map { dimIdx =>
      val cellBits = signature.get(dimIdx * numberOfBitsPerDimension, (dimIdx + 1) * numberOfBitsPerDimension)
      cellBits.toLong.toInt
    }.reverse
  }
}
