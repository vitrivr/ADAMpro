package ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.signature

import java.util.BitSet

import ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.VectorApproximationIndexer.Signature

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
class FixedSignatureGenerator(numberOfDimensions: Int, numberOfBitsPerDimension: Int) extends SignatureGenerator with Serializable {
  
  /**
   * 
   */
  def toSignature(cells: Seq[Int]): Signature = {
//    require(cells.forall { cell => numberOfBitsPerDimension <= math.max(1.0, math.ceil(math.log(cell) / math.log(2))) })
//    require(cells.size == numberOfDimensions)

    val bits = new BitSet(numberOfDimensions * numberOfBitsPerDimension)

    cells.reverse.zipWithIndex.foreach {
      case (cell, dimIdx) =>
        val cellBits = BitSet.valueOf(Array(cell.toLong << (dimIdx * numberOfBitsPerDimension)))
        bits.or(cellBits)
    }

    bits.toByteArray()
  }

  /**
   * 
   */
  def toCells(signature: Signature): Seq[Int] = {
    val bits = BitSet.valueOf(signature)

    (0 until numberOfDimensions).map { dimIdx =>
      val cellBits = bits.get(dimIdx * numberOfBitsPerDimension, (dimIdx + 1) * numberOfBitsPerDimension)
      cellBits.toLongArray()(0).toInt
    }.reverse
  }
}
