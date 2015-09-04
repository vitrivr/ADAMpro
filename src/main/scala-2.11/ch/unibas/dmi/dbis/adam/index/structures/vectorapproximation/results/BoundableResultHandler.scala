package ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.results

import ch.unibas.dmi.dbis.adam.data.IndexTuple
import ch.unibas.dmi.dbis.adam.data.types.bitString.BitString._
import ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.VectorApproximationIndexer.Bounds
import ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.signature.SignatureGenerator
import ch.unibas.dmi.dbis.adam.query.distance.Distance.Distance

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
class BoundableResultHandler(k: Int, lbounds: Bounds = null, ubounds: Bounds = null, signatureGenerator: SignatureGenerator = null) extends ResultHandler(k) {
  
  /**
   * 
   */
  class BoundableResultElement(val indexTuple: IndexTuple[BitStringType]) extends ResultElement {
    lazy val ubound = computeBounds(ubounds, indexTuple.value)
    val lbound = computeBounds(lbounds, indexTuple.value)

    private def computeBounds(bounds: Bounds, signature: BitStringType): Distance = {
      computeBounds(bounds, signatureGenerator.toCells(signature))
    }

    private def computeBounds(bounds: Bounds, cells: Seq[Int]): Distance = {
      var sum = 0.toFloat
      (bounds zip cells).foreach {
        case (dimBounds, index) =>
          sum += dimBounds(index)
      }
      sum
    }
  }

  /**
   * 
   */
  def offerIndexTuple(it: Iterator[IndexTuple[BitStringType]]): Unit = {
    super.offerResultElement(it.map { new BoundableResultElement(_) })
  }
}
