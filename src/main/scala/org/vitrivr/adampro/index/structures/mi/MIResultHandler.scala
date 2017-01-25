package org.vitrivr.adampro.index.structures.mi

import com.google.common.collect.MinMaxPriorityQueue
import org.vitrivr.adampro.datatypes.TupleID.TupleID
import org.vitrivr.adampro.query.distance.Distance.Distance
import org.vitrivr.adampro.datatypes.vector.Vector
import org.vitrivr.adampro.query.distance.Distance

import scala.collection.mutable

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2016
  */
private[mi] class MIResultHandler(ki: Int, k: Int) {
  @transient private val objectsOfOrderedRes = MinMaxPriorityQueue.orderedBy(Ordering[MIResultElement].reverse).maximumSize(k).create[MIResultElement]()
  @transient protected val orderedRes = mutable.Map[TupleID, MIResultElement]()
  @transient protected val tempRes = mutable.Map[TupleID, MIResultElement]()

  val maxEntries = ki

  var kDist = Distance.maxValue

  /**
    *
    * @param ap_id
    * @param score
    * @param position
    * @param postingListsToBeAccessed
    * @param accessedPostingLists
    */
  def put(ap_id: TupleID, score: Int, position: Int, postingListsToBeAccessed: Int, accessedPostingLists: Int): Unit = {
    val increment = Math.abs(position - score)
    var currentScore = Distance.zeroValue

    val i = tempRes.get(ap_id)

    if (i.isDefined) {
      currentScore = i.get.ap_score - maxEntries + increment
      i.get.ap_score = currentScore
    } else {
      currentScore = maxEntries * (postingListsToBeAccessed - 1) + increment
    }

    val minDist = currentScore - maxEntries * (postingListsToBeAccessed - accessedPostingLists) // the minimum distance this object can reach

    if (minDist > kDist) {
      tempRes.remove(ap_id)
    } else {
      //the object can enter the best k, so insert it
      if (i.isEmpty) {
        tempRes += ap_id -> MIResultElement(ap_id, currentScore)
      }
      orderedInsert(ap_id, currentScore)
    }
  }

  /**
    *
    * @param ap_id
    * @param ap_score
    */
  private def orderedInsert(ap_id: TupleID, ap_score: Distance): Unit = {
    val res = MIResultElement(ap_id, ap_score)
    orderedRes += ap_id -> res
    objectsOfOrderedRes.add(res)
    kDist = objectsOfOrderedRes.peekLast().ap_score
  }

  /**
    *
    * @return
    */
  def results = orderedRes.values.toSeq
}

case class MIResultElement(ap_id: TupleID, var ap_score: Distance) extends Ordered[MIResultElement] {
  def compare(that: MIResultElement): Int = this.ap_score.compare(that.ap_score)
}