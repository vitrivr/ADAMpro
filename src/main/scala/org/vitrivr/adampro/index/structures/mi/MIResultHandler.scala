package org.vitrivr.adampro.index.structures.mi

import org.vitrivr.adampro.query.distance.Distance.Distance
import com.google.common.collect.MinMaxPriorityQueue

import scala.collection.mutable

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2016
  */
private[mi] class MIResultHandler[A](ki: Int, k: Int) {
  @transient private var objectsOfOrderedRes : MinMaxPriorityQueue[MIResultElement[A]] = MinMaxPriorityQueue.orderedBy(Ordering[MIResultElement[A]].reverse).maximumSize(k).create()
  @transient protected var orderedRes = mutable.Map[A, MIResultElement[A]]()
  @transient protected var tempRes = mutable.Map[A, MIResultElement[A]]() //TODO: possibly remove tempRes; it has been added to be similar to the original code, but it does not seem to be necessary

  val maxEntries = ki

  var kDist = Float.MaxValue

  /**
    *
    * @param tid
    * @param score
    * @param position
    * @param postingListsToBeAccessed
    * @param accessedPostingLists
    */
  def put(tid: A, score: Int, position: Int, postingListsToBeAccessed: Int, accessedPostingLists: Int): Unit = {
    val increment = Math.abs(position - score)
    var currentScore = 0.toFloat

    val i = tempRes.get(tid)

    if (i.isDefined) {
      currentScore = i.get.score - maxEntries + increment
      i.get.score = currentScore
    } else {
      currentScore = maxEntries * (postingListsToBeAccessed - 1) + increment
    }

    val minDist = currentScore - maxEntries * (postingListsToBeAccessed - accessedPostingLists) // the minimum distance this object can reach

    if (minDist > kDist) {
      tempRes.remove(tid)
    } else {
      //the object can enter the best k, so insert it
      if (i.isEmpty) {
        tempRes += tid -> MIResultElement(currentScore, tid)
      }
      orderedInsert(tid, currentScore)
    }
  }

  /**
    *
    * @param tid
    * @param dist
    */
  private def orderedInsert(tid: A, dist: Distance): Unit = {
    val res = MIResultElement(dist, tid)
    orderedRes += tid -> res
    objectsOfOrderedRes.add(res)
    kDist = objectsOfOrderedRes.peekLast().score
  }

  /**
    *
    * @return
    */
  def results = orderedRes.values.toSeq
}

case class MIResultElement[A](var score: Float, tid: A) extends Ordered[MIResultElement[A]] {
  def compare(that: MIResultElement[A]): Int = this.score.compare(that.score)
}