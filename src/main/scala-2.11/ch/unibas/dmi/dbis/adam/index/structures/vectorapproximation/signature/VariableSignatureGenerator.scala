package ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.signature

import java.util.BitSet

import ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.VectorApproximationIndexer.{Bounds, Signature, Marks}

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
class VariableSignatureGenerator(numberOfBitsPerDimension: Seq[Int]) extends Serializable {
  /**
   * 
   */
  def toSignature(cells: Seq[Int]): Signature = {
    require(cells.size == numberOfBitsPerDimension.size)
    require(cells.zip(numberOfBitsPerDimension).forall { case (cell, bits) => bits < math.max(1.0, math.ceil(math.log(cell) / math.log(2))) })

    val cumulativeNumberOfBitsPerDimension = numberOfBitsPerDimension.scanLeft(0)(_ + _).dropRight(1)

    val bits = new BitSet(numberOfBitsPerDimension.sum)

    cells.reverse.zip(cumulativeNumberOfBitsPerDimension).foreach {
      case (cell, nBits) =>
        val cellBits = BitSet.valueOf(Array(cell.toLong << nBits))
        bits.or(cellBits)
    }

    bits.toByteArray()
  }

  /**
   * 
   */
  def toCells(signature: Signature): Seq[Int] = {
    val bits = BitSet.valueOf(signature)

    val cumulativeNumberOfBitsPerDimension = numberOfBitsPerDimension.scanLeft(0)(_ + _)

    cumulativeNumberOfBitsPerDimension.toList.sliding(2).map {
      case List(start, end) =>
        val cellBits = bits.get(start, end)
        cellBits.toLongArray()(0).toInt
    }.toList.reverse
  }
}
