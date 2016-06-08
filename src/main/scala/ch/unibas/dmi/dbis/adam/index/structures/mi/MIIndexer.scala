package ch.unibas.dmi.dbis.adam.index.structures.mi

import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.index.{Index, IndexGenerator, IndexingTaskTuple}
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.distance.DistanceFunction
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.util.random.Sampling

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2016
  */
class MIIndexer(p_ki: Option[Int], p_ks: Option[Int], distance: DistanceFunction, nrefs: Option[Int])(@transient implicit val ac: AdamContext) extends IndexGenerator {
  override val indextypename: IndexTypeName = IndexTypes.MIINDEX

  /**
    *
    * @param indexname  name of index
    * @param entityname name of entity
    * @param data       data to index
    * @return
    */
  override def index(indexname: IndexName, entityname: EntityName, data: RDD[IndexingTaskTuple[_]]): Index = {
    val entity = Entity.load(entityname).get

    val n = entity.count
    val fraction = Sampling.computeFractionForSampleSize(math.max(nrefs.getOrElse(math.ceil(2 * math.sqrt(n)).toInt), IndexGenerator.MINIMUM_NUMBER_OF_TUPLE), n, withReplacement = false)
    var trainData = data.sample(false, fraction).collect()
    if (trainData.length < IndexGenerator.MINIMUM_NUMBER_OF_TUPLE) {
      trainData = data.take(IndexGenerator.MINIMUM_NUMBER_OF_TUPLE)
    }
    assert(trainData.length >= math.ceil(2 * math.sqrt(n)).toInt) //as discussed in the paper

    val refs = ac.sc.broadcast(trainData.zipWithIndex.map { case (idt, idx) => IndexingTaskTuple(idx, idt.feature) })

    val ki = p_ki.getOrElse(math.min(100, refs.value.length))
    val ks = p_ks.getOrElse(math.min(100, refs.value.length))

    assert(ks <= ki)

    log.trace("MI index chosen " + refs.value.length + " reference points")

    log.debug("MI indexing...")

    case class ReferencePointAssignment[A](tid: A, refid: Int, score: Int)

    val indexdata = data.flatMap(datum => {
      refs.value
        .sortBy(ref => distance.apply(datum.feature, ref.feature)) //sort refs by distance
        .zipWithIndex //give rank (score)
        .map { case (ref, idx) => ReferencePointAssignment(datum.id, ref.id, idx) } //obj, postingListId, score
        .take(ki) //limit to number of nearest pivots used when indexing
    }).groupBy(_.refid) //group by id of pivot (create posting list)
      .mapValues(vals => vals.toSeq.sortBy(_.score)) //order by score
      .mapValues(vals => (vals.map(_.tid), vals.map(_.score))) //postingListId, scoreList
      .map(x => Row(x._1, x._2._1, x._2._2))


    val schema = StructType(Seq(
      StructField(MIIndexer.REFERENCE_OBJ_NAME, IntegerType, nullable = false),
      StructField(MIIndexer.POSTING_LIST_NAME, new ArrayType(entity.pk.fieldtype.datatype, false), nullable = false),
      StructField(MIIndexer.SCORE_LIST_NAME, new ArrayType(IntegerType, false), nullable = false)
    ))

    val df = ac.sqlContext.createDataFrame(indexdata, schema)

    MIIndex(indexname, entityname, df, MIIndexMetaData(ki, ks, refs.value))
  }
}

object MIIndexer {
  def apply(distance: DistanceFunction, properties: Map[String, String] = Map[String, String]())(implicit ac: AdamContext): IndexGenerator = {
    val ki = properties.get("ki").map(_.toInt)
    val ks = properties.get("ks").map(_.toInt)
    val trainingSize = properties.get("nrefs").map(_.toInt)

    new MIIndexer(ki, ks, distance, trainingSize)
  }

  //names of fields to store data of index
  private[mi] val REFERENCE_OBJ_NAME = "ref"
  private[mi] val POSTING_LIST_NAME = "postings"
  private[mi] val SCORE_LIST_NAME = "scores"
}