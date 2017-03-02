package org.vitrivr.adampro.index.structures.mi

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.{DataFrame, Row}
import org.vitrivr.adampro.config.AttributeNames
import org.vitrivr.adampro.datatypes.TupleID.TupleID
import org.vitrivr.adampro.datatypes.vector.Vector._
import org.vitrivr.adampro.helpers.tracker.OperationTracker
import org.vitrivr.adampro.index.Index
import org.vitrivr.adampro.index.Index._
import org.vitrivr.adampro.index.structures.IndexTypes
import org.vitrivr.adampro.main.AdamContext
import org.vitrivr.adampro.query.Result
import org.vitrivr.adampro.query.distance.DistanceFunction
import org.vitrivr.adampro.query.query.NearestNeighbourQuery

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2016
  */
@Experimental class MIIndex(override val indexname: IndexName)(@transient override implicit val ac: AdamContext)
  extends Index(indexname)(ac) {

  override val indextypename: IndexTypeName = IndexTypes.MIINDEX
  override val lossy: Boolean = true
  override val confidence = 0.5.toFloat

  val meta = metadata.get.asInstanceOf[MIIndexMetaData]


  override def scan(data: DataFrame, q: MathVector, distance: DistanceFunction, options: Map[String, String], k: Int)(tracker : OperationTracker): DataFrame = {
    log.debug("scanning MI index " + indexname)

    val ki = meta.ki
    //ks is the number of closest reference points to consider
    val ks = options.mapValues(_.toInt).getOrElse("ks", meta.ks)
    assert(ks <= ki)

    //maximum position difference, MPD, access just pairs whose position difference is below a threshold
    val max_pos_diff = options.mapValues(_.toInt).getOrElse("max_pos_diff", ki)

    //take closest ks reference points
    val q_knp = meta.refs.sortBy(ref => distance(ref.ap_indexable, q)).take(ks)

    log.trace("reference points prepared")

    val rh = new MIResultHandler(ki + 1, k)

    q_knp.zipWithIndex.foreach {
      case (e, pos) =>
        val piv = e.ap_id //pivot = id of reference object

        val low_inv_file_pos = pos - max_pos_diff
        val high_inv_file_pos = math.min(pos + max_pos_diff, ki)

        log.trace("reading next posting list")
        val entries = getPostingList(data, piv, low_inv_file_pos, high_inv_file_pos)

        entries.foreach { item =>
          val tid = item._1
          val score = item._2
          rh.put(tid, score, pos, q_knp.size, pos + 1)
        }
    }

    log.debug("MI index returning " + rh.results.length + " results")

    val results = ac.sc.parallelize(rh.results.map(x => Row(x.ap_id, x.ap_score)))
    ac.sqlContext.createDataFrame(results, Result.resultSchema)
  }

  /**
    * Returns posting list.
    *
    * @param data dataframe to retrieve posting list from
    * @param piv pivot
    * @param low_inv_file_pos lowest score
    * @param high_inv_file_pos highest score
    * @return
    */
  private def getPostingList(data: DataFrame, piv: TupleID, low_inv_file_pos: Int, high_inv_file_pos: Int): Seq[(TupleID, Int)] = {
    val entries = data.filter(data(MIIndex.REFERENCE_OBJ_NAME) === piv).select(MIIndex.POSTING_LIST_NAME, MIIndex.SCORE_LIST_NAME).collect()

    val postings = entries.map(_.getAs[Seq[TupleID]](MIIndex.POSTING_LIST_NAME)).head
    val scores = entries.map(_.getAs[Seq[Int]](MIIndex.SCORE_LIST_NAME)).head

    val low = math.min(0, low_inv_file_pos)
    val high = math.max(entries.length, high_inv_file_pos)

    (postings zip scores).slice(low, high)
  }

  override def isQueryConform(nnq: NearestNeighbourQuery): Boolean = true
}

object MIIndex {
  //names of fields to store data of index
  private[mi] val REFERENCE_OBJ_NAME = "ref"
  private[mi] val POSTING_LIST_NAME = "postings"
  private[mi] val SCORE_LIST_NAME = "scores"
}