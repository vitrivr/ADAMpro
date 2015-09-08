package ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation

import scala.collection.IterableLike

/**
 *
 */
package object marks {

  /**
   *
   */
  implicit class IterableLikeExtension[A, Repr <: IterableLike[A, Repr]](val iterableLike: IterableLike[A, Repr]) extends AnyVal {

    /**
     *
     */
    def foldLeftWhile[B](z: B)(p: B => Boolean)(op: (B, A) => B): B = {
      var result = z
      val it = iterableLike.iterator
      while (it.hasNext && p(result)) {
        val next = it.next()
        result = op(result, next)
      }
      result
    }

    /**
     *
     */
    def foldLeftWhileCounting[B](z: B)(p: B => Boolean)(op: (B, A) => B): (Long, B) = {
      var result = z
      var i: Long = 0
      val it = iterableLike.iterator
      while (it.hasNext) {
        val next = it.next()
        result = op(result, next)

        if (!p(result)) {
          return (i, result)
        } else {
          i += 1
        }
      }

      return (i, result)
    }
  }

}

