package ch.unibas.dmi.dbis.adam.index.structures.va.results

import ch.unibas.dmi.dbis.adam.datatypes.bitString.BitString
import ch.unibas.dmi.dbis.adam.entity.Tuple.TupleID
import ch.unibas.dmi.dbis.adam.index.BitStringIndexTuple
import ch.unibas.dmi.dbis.adam.index.structures.va.VAIndex._
import ch.unibas.dmi.dbis.adam.index.structures.va.signature.SignatureGenerator
import ch.unibas.dmi.dbis.adam.index.utils.FixedSizePriorityQueue
import ch.unibas.dmi.dbis.adam.query.distance.Distance.Distance

import scala.collection.mutable.ListBuffer

/**
  * adamtwo
  *
  * Ivan Giangreco
  * August 2015
  */
private[va] class VAResultHandler(k: Int, lbounds: => Bounds = null, ubounds: => Bounds = null, signatureGenerator: SignatureGenerator = null) extends Serializable {
  @transient private var ls = ListBuffer[ResultElement]()
  @transient private var queue = new FixedSizePriorityQueue[Float](k)(scala.math.Ordering.Float.reverse)

  def iterator: Iterator[ResultElement] = results.iterator

  def results = ls.sortBy(_.ubound)

  /**
    *
    * @param indexTuple
    */
  def offer(indexTuple: BitStringIndexTuple): Unit = offer(BoundedResultElement(indexTuple.id, indexTuple.value))


  /**
    *
    * @param res
    */
  def offer(res: ResultElement): Unit = {
    if (queue.offer(res.lbound, res.ubound)) {
      ls.+=(res)
    }
  }


  private case class BoundedResultElement(val tid: TupleID, @transient bits: BitString[_]) extends ResultElement {
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


