package ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.results

import ch.unibas.dmi.dbis.adam.datatypes.bitString.BitString
import ch.unibas.dmi.dbis.adam.index.IndexTuple
import ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.VectorApproximationIndex._
import ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.signature.SignatureGenerator
import ch.unibas.dmi.dbis.adam.query.distance.Distance.Distance
import com.google.common.collect.MinMaxPriorityQueue

import scala.collection.mutable.ListBuffer

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
private[vectorapproximation] class VectorApproximationResultHandler(k: Int, lbounds: => Bounds = null, ubounds: => Bounds = null, signatureGenerator: SignatureGenerator = null) extends Serializable {
  @transient private var ls =  ListBuffer[ResultElement]()
  @transient private var queue = MinMaxPriorityQueue.orderedBy(scala.math.Ordering.Float).maximumSize(k).create[Float]
  @transient private var max = Float.MaxValue

  /**
   *
   * @param it
   */
  def offerIndexTuple(it: Iterator[IndexTuple]): Unit = {
    while(it.hasNext){
      val indexTuple = it.next()
      val res = BoundedResultElement(indexTuple)
      if (res.lbound < max || queue.size < k) {
        ls.+=(res)
        queue.add(res.ubound)
        max = queue.peekLast()
      }
    }
  }


  /**
   *
   * @param tuple
   */
  def offerIndexTuple(tuple : IndexTuple): Unit = {
    offerResultElement(BoundedResultElement(tuple))
  }


  /**
   *
   * @param it
   */
  def offerResultElement(it: Iterator[ResultElement]): Unit = {
    while(it.hasNext){
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
  def offerResultElement(res : ResultElement): Unit = {
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



 case class BoundedResultElement(val indexTuple: IndexTuple) extends ResultElement {
    val lbound : Distance = computeBounds(lbounds, indexTuple.bits)
    lazy val ubound : Distance = computeBounds(ubounds, indexTuple.bits)

    /**
     *
     * @param bounds
     * @param signature
     * @return
     */
    @inline private def computeBounds(bounds: => Bounds, signature: BitString[_]): Distance = {
      val cells = signatureGenerator.toCells(signature)

      var sum : Float = 0
      var idx = 0
      while(idx < cells.length){
        sum += bounds(idx)(cells(idx))
        idx += 1
      }
      sum
    }
  }
}

trait ResultElement {
  def indexTuple : IndexTuple
  val lbound : Distance
  val ubound : Distance
}
