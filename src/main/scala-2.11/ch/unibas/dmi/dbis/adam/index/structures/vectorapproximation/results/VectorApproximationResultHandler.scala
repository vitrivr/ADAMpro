package ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.results

import ch.unibas.dmi.dbis.adam.datatypes.bitString.BitString
import ch.unibas.dmi.dbis.adam.index.BitStringIndexTuple
import ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.VectorApproximationIndex._
import ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.signature.SignatureGenerator
import ch.unibas.dmi.dbis.adam.query.distance.Distance.Distance
import ch.unibas.dmi.dbis.adam.table.Tuple.TupleID
import com.google.common.collect.MinMaxPriorityQueue

import scala.collection.mutable.ListBuffer

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
private[vectorapproximation] class VectorApproximationResultHandler(k: Int, lbounds: => Bounds = null, ubounds: => Bounds = null, signatureGenerator: SignatureGenerator = null) extends Serializable {
  @transient private var ls = ListBuffer[ResultElement]()
  @transient private var queue = MinMaxPriorityQueue.orderedBy(scala.math.Ordering.Float).maximumSize(k).create[Float]
  @transient private var max = Float.MaxValue

  /**
   *
   * @param it
   */
  def offerIndexTuple(it: Iterator[BitStringIndexTuple]): Unit = {
    while (it.hasNext) {
      val indexTuple = it.next()
      val res = BoundedResultElement(indexTuple.tid, indexTuple.bits)
      if (res.lbound < max || queue.size < k) {
        ls.+=(res)
        queue.add(res.ubound)
        max = queue.peekLast()
      }
    }
  }


  /**
   *
   * @param indexTuple
   */
  def offerIndexTuple(indexTuple: BitStringIndexTuple): Unit = {
    offerResultElement(BoundedResultElement(indexTuple.tid, indexTuple.bits))
  }


  /**
   *
   * @param it
   */
  def offerResultElement(it: Iterator[ResultElement]): Unit = {
    while (it.hasNext) {
      val res = it.next()
      if (res.lbound < max || queue.size < k) {
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
  def offerResultElement(res: ResultElement): Unit = {
    if (res.lbound < max || queue.size < k) {
      ls.+=(res)
      queue.add(res.ubound)
      max = queue.peekLast()
    }
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
    ls.sortBy(_.ubound)
  }


  case class BoundedResultElement(val tid: TupleID, @transient bits: BitString[_]) extends ResultElement {

    val lbound: Distance = computeBounds(lbounds, bits)
    lazy val ubound: Distance = computeBounds(ubounds, bits)

    /**
     *
     * @param bounds
     * @param signature
     * @return
     */
    @inline private def computeBounds(bounds: => Bounds, signature: BitString[_]): Distance = {
      val cells = signatureGenerator.toCells(signature)

      var sum: Float = 0
      var idx = 0
      while (idx < cells.length) {
        sum += bounds(idx)(cells(idx))
        idx += 1
      }
      sum
    }
  }

}

trait ResultElement {
  val tid: TupleID
  val lbound: Distance
  val ubound: Distance
}
