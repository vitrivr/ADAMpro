package org.vitrivr.adampro.index.structures.mi

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.vitrivr.adampro.config.AttributeNames
import org.vitrivr.adampro.datatypes.TupleID
import org.vitrivr.adampro.datatypes.TupleID._
import org.vitrivr.adampro.datatypes.vector.Vector
import org.vitrivr.adampro.datatypes.vector.Vector._
import org.vitrivr.adampro.helpers.tracker.OperationTracker
import org.vitrivr.adampro.index.Index._
import org.vitrivr.adampro.index.structures.IndexTypes
import org.vitrivr.adampro.index.{IndexGenerator, IndexGeneratorFactory, IndexingTaskTuple, ParameterInfo}
import org.vitrivr.adampro.main.AdamContext
import org.vitrivr.adampro.query.distance.DistanceFunction

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2016
  */
@Experimental class MIIndexGenerator(p_ki: Option[Int], p_ks: Option[Int], distance: DistanceFunction, nrefs: Option[Int])(@transient implicit val ac: AdamContext) extends IndexGenerator {
  override val indextypename: IndexTypeName = IndexTypes.MIINDEX

  /**
    *
    * @param data raw data to index
    * @return
    */
  override def index(data: DataFrame, attribute: String)(tracker : OperationTracker): (DataFrame, Serializable) = {
    log.trace("LSH started indexing")

    val sample = getSample(math.max(nrefs.getOrElse(math.ceil(2 * math.sqrt(data.count())).toInt), MINIMUM_NUMBER_OF_TUPLE), attribute)(data)

    val refsBc = ac.sc.broadcast(sample.zipWithIndex.map { case (idt, idx) => IndexingTaskTuple(idx.toLong, idt.ap_indexable) })
    tracker.addBroadcast(refsBc)

    val ki = p_ki.getOrElse(math.min(100, refsBc.value.length))
    val ks = p_ks.getOrElse(math.min(100, refsBc.value.length))
    assert(ks <= ki)
    log.trace("MI index chosen " + refsBc.value.length + " reference points")

    case class ReferencePointAssignment(tid: TupleID, refid: TupleID, score: Int)

    val indexed = data.rdd
      .map(r => IndexingTaskTuple(r.getAs(AttributeNames.internalIdColumnName), Vector.conv_dspark2vec(r.getAs[DenseSparkVector](attribute))))
      .flatMap(datum => {
        refsBc.value
          .sortBy(ref => distance.apply(datum.ap_indexable, ref.ap_indexable)) //sort refs by distance
          .zipWithIndex //give rank (score)
          .map { case (ref, idx) => ReferencePointAssignment(datum.ap_id, ref.ap_id, idx) } //obj, postingListId, score
          .take(ki) //limit to number of nearest pivots used when indexing
      }).groupBy(_.refid) //group by id of pivot (create posting list)
      .mapValues(vals => vals.toSeq.sortBy(_.score)) //order by score
      .mapValues(vals => (vals.map(_.tid), vals.map(_.score))) //postingListId, scoreList
      .map(x => Row(x._1, x._2._1, x._2._2))


    //TODO: possibly change structure to fit better Spark and return a ReferencePointAssignment rather than a true posting list
    /*val indexed = data.rdd.flatMap(r => {
      refsBc.value
        .sortBy(ref => distance.apply(Vector.conv_dspark2vec(r.getAs[DenseSparkVector](attribute)), ref.ap_indexable)) //sort refs by distance
        .zipWithIndex //give rank (score)
        .map { case (ref, idx) => ReferencePointAssignment(r.getAs(AttributeNames.internalIdColumnName), ref.ap_id, idx) } //obj, postingListId, score
        .take(ki) //limit to number of nearest pivots used when indexing
    }).map(x => Row(x.refid, x.tid, x.score))*/


    log.trace("MI finished indexing")

    val schema = StructType(Seq(
      StructField(MIIndex.REFERENCE_OBJ_NAME, TupleID.SparkTupleID, nullable = false),
      StructField(MIIndex.POSTING_LIST_NAME, new ArrayType(data.schema.fields.apply(0).dataType, false), nullable = false),
      StructField(MIIndex.SCORE_LIST_NAME, new ArrayType(IntegerType, false), nullable = false)
    ))

    val df = ac.sqlContext.createDataFrame(indexed, schema)
    val meta = MIIndexMetaData(ki, ks, refsBc.value)

    (df, meta)
  }
}


class MIIndexGeneratorFactory extends IndexGeneratorFactory {
  /**
    * @param distance   distance function
    * @param properties indexing properties
    */
  def getIndexGenerator(distance: DistanceFunction, properties: Map[String, String] = Map[String, String]())(implicit ac: AdamContext): IndexGenerator = {
    val ki = properties.get("ki").map(_.toInt)
    val ks = properties.get("ks").map(_.toInt)

    val nrefs = properties.get("nrefs").map(_.toInt)

    new MIIndexGenerator(ki, ks, distance, nrefs)
  }

  /**
    *
    * @return
    */
  override def parametersInfo: Seq[ParameterInfo] = Seq(
    new ParameterInfo("nrefs", "number of reference objects", Seq(64, 128, 256, 512).map(_.toString)),
    new ParameterInfo("ki", "number of reference objects used for indexing", Seq(64, 128, 256, 512).map(_.toString)),
    new ParameterInfo("ks", "number of reference objects used for searching (ks <= ki)", Seq(64, 128, 256, 512).map(_.toString))
  )
}