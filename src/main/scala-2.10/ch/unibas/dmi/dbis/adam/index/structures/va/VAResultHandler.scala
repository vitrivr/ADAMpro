package ch.unibas.dmi.dbis.adam.index.structures.va

import ch.unibas.dmi.dbis.adam.datatypes.bitString.BitString
import ch.unibas.dmi.dbis.adam.entity.Tuple.TupleID
import ch.unibas.dmi.dbis.adam.index.BitStringIndexTuple
import ch.unibas.dmi.dbis.adam.index.structures.va.VAIndex._
import ch.unibas.dmi.dbis.adam.index.structures.va.signature.SignatureGenerator
import ch.unibas.dmi.dbis.adam.index.utils.{ResultElement, ResultHandler}
import ch.unibas.dmi.dbis.adam.query.distance.Distance.Distance

/**
  * adamtwo
  *
  * Ivan Giangreco
  * August 2015
  */
private[va] class VAResultHandler(k: Int, lbounds: => Bounds = null, ubounds: => Bounds = null, signatureGenerator: SignatureGenerator = null) extends ResultHandler[Float](k)(scala.math.Ordering.Float.reverse) {

  /**
    *
    * @param indexTuple
    */
  def offer(indexTuple: BitStringIndexTuple): Unit = offer(new BoundedResultElement(indexTuple.id, indexTuple.value))


  class BoundedResultElement(override val tid: TupleID, @transient bits: BitString[_]) extends ResultElement(0.toFloat, tid) {
    override def compareScore = lbound
    override def insertScore = ubound

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