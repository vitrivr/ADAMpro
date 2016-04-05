package ch.unibas.dmi.dbis.adam.index.utils

import java.util.PriorityQueue

/**
  * adampro
  *
  * Ivan Giangreco
  * April 2016
  */
class FixedSizePriorityQueue[A](val maxSize: Int)(implicit val ordering: Ordering[A]) {
  private val queue = new PriorityQueue[A](maxSize, ordering)
  private var elementsLeft = maxSize

  /**
    *
    * @param comparing element which is compared to queue
    * @param inserting element which is inserted to queue if accepted after comparison
    * @return
    */
  def offer(comparing: A, inserting: A): Boolean = {
    queue.synchronized {
      if (elementsLeft > 0) {
        queue.add(inserting)
        elementsLeft -= 1
        return true
      } else {
        val peek = queue.peek()
        if (ordering.lteq(peek, comparing)) {
          queue.poll()
          queue.add(inserting)
          return true
        } else {
          return false
        }
      }
    }
  }

  /**
    *
    * @param e
    * @return
    */
  def offer(e: A): Boolean = offer(e, e)
}
