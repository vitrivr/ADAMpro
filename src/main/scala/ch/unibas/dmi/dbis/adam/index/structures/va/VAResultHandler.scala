package ch.unibas.dmi.dbis.adam.index.structures.va

import java.util.Comparator

import it.unimi.dsi.fastutil.floats.{FloatComparator, FloatComparators, FloatHeapPriorityQueue}
import it.unimi.dsi.fastutil.objects.ObjectHeapPriorityQueue
import org.apache.spark.sql.Row

import scala.collection.mutable.ListBuffer

/**
  * adamtwo
  *
  * Ivan Giangreco
  * August 2015
  */
private[va] class VAResultHandler[A](k: Int) {
  @transient private var elementsLeft = k
  @transient private val queue =  new FloatHeapPriorityQueue(2 * k, FloatComparators.OPPOSITE_COMPARATOR)

  @SerialVersionUID(1L)
  protected class VAComparator(var comparator : FloatComparator) extends Comparator[VAResultElement[A]] with java.io.Serializable {
    final def compare(a: VAResultElement[A], b: VAResultElement[A]): Int = comparator.compare(a.lower, b.lower)
  }
  @transient private val objQueue = new ObjectHeapPriorityQueue[VAResultElement[A]](new VAComparator(FloatComparators.OPPOSITE_COMPARATOR))
  @transient protected var ls = ListBuffer[VAResultElement[A]]()

  /**
    *
    * @param r
    */
  def offer(r: Row, pk : String): Boolean = {
    queue.synchronized {
      if (elementsLeft > 0) { //we have not yet inserted k elements, no checks therefore
        val lower = r.getAs[Float]("lbound")
        val upper = r.getAs[Float]("ubound")
        val tid = r.getAs[A](pk)
        elementsLeft -= 1
        enqueueAndAddToCandidates(lower, upper, tid)
        return true
      } else { //we have already k elements, therefore check if new element is better
        //peek is the upper bound
        val peek = queue.firstFloat()
        val lower = r.getAs[Float]("lbound")
        if (peek >= lower) {
          //if peek is larger than lower, then dequeue worst element and insert
          // TODO Who gives us a guarantee that the enqueued upper bound is bigger than the dequeued
          //new element
          queue.dequeueFloat()
          val upper = r.getAs[Float]("ubound")
          val tid = r.getAs[A](pk : String)
          enqueueAndAddToCandidates(lower, upper, tid)
          return true
        } else {
          return false
        }
      }
    }
  }

  /**
    *
    * @param lower
    * @param upper
    * @param tid
    */
  private def enqueueAndAddToCandidates(lower : Float, upper : Float, tid : A): Unit ={
    enqueueAndAddToCandidates(VAResultElement(lower, upper, tid))
  }

  /**
    *
    * @param res
    */
  private def enqueueAndAddToCandidates(res: VAResultElement[A]): Unit = {
    queue.enqueue(res.upper)
    objQueue.enqueue(res)
    while(objQueue.first().lower>queue.firstFloat()){
      objQueue.dequeue()
    }
    //ls += res
  }


  /**
    *
    * @return
    */
  def results = {
    while(objQueue.size()>0){
      ls+=objQueue.dequeue()
    }
    ls.toSeq
  }

}

case class VAResultElement[A](lower: Float, upper : Float, tid: A) {}