package ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.results

import ch.unibas.dmi.dbis.adam.datatypes.bitString.BitString
import ch.unibas.dmi.dbis.adam.index.IndexTuple
import ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.VectorApproximationIndex._
import ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.signature.SignatureGenerator
import ch.unibas.dmi.dbis.adam.query.distance.Distance.Distance
import com.google.common.collect.MinMaxPriorityQueue

import scala.collection.mutable.ArrayBuffer

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
private[vectorapproximation] class VectorApproximationResultHandler(k: Int, lbounds: Bounds = null, ubounds: Bounds = null, signatureGenerator: SignatureGenerator = null) extends Serializable {
  @transient private var ls = new ArrayBuffer[ResultElement]()
  @transient private var queue = MinMaxPriorityQueue.orderedBy(scala.math.Ordering.Float).maximumSize(k).create[Float]
  @transient private var max = Float.MaxValue

   /**
   *
   * @param it
   */
  def offerResultElement(it: Iterator[ResultElement]): Unit = {
    it.foreach { res =>
      if (res.lbound < max || ls.size < k) {
        ls.+=(res)
        queue.add(res.ubound)
        max = queue.peekLast()
      }
    }
  }

  /**
   *
   * @param res
   */
  def offerResultElement(res : ResultElement): Unit = {
    if (res.lbound < max || ls.size < k) {
      ls.+=(res)
      queue.add(res.ubound)
      max = queue.peekLast()
    }
  }

  /**
   *
   * @param tuple
   */
  def offerResultElement(tuple : IndexTuple): Unit = {
    offerResultElement(BoundedResultElement(tuple))
  }

  /**
   *
   * @return
   */
  def iterator: Iterator[ResultElement] = {
    results.iterator
  }

  /**
   *
   * @return
   */
  def results = {
    ls
  }



 case class BoundedResultElement(val indexTuple: IndexTuple) extends ResultElement {
    lazy val lbound : Distance = computeBounds(lbounds, indexTuple.bits)
    lazy val ubound : Distance = computeBounds(ubounds, indexTuple.bits)

    /**
     *
     * @param bounds
     * @param signature
     * @return
     */
    private def computeBounds(bounds: Bounds, signature: BitString[_]): Distance = {
      val cells = signatureGenerator.toCells(signature)

      var sum : Float = 0
      cells.zipWithIndex.foreach { case(cell, index) =>
        sum += bounds(index)(cell)
      }

      sum
    }
  }
}

trait ResultElement {
  def indexTuple : IndexTuple
  def lbound : Distance
  def ubound : Distance
}
