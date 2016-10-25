package ch.unibas.dmi.dbis.adam.index.structures.mi

import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.index.{ParameterInfo, IndexGeneratorFactory, IndexGenerator, IndexingTaskTuple}
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.distance.DistanceFunction
import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.util.random.Sampling

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
    * @param indexname  name of index
    * @param entityname name of entity
    * @param data       data to index
    * @return
    */
  override def index(indexname: IndexName, entityname: EntityName, data: RDD[IndexingTaskTuple[_]]): (DataFrame, Serializable) = {
    val entity = Entity.load(entityname).get

    val n = entity.count
    val fraction = Sampling.computeFractionForSampleSize(math.max(nrefs.getOrElse(math.ceil(2 * math.sqrt(n)).toInt), MINIMUM_NUMBER_OF_TUPLE), n, withReplacement = false)
    var trainData = data.sample(false, fraction).collect()
    if (trainData.length < MINIMUM_NUMBER_OF_TUPLE) {
      trainData = data.take(MINIMUM_NUMBER_OF_TUPLE)
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

    //TODO: possibly change structure to fit better Spark and return a ReferencePointAssignment rather than a true posting list
    /*val indexdata = data.flatMap(datum => {
      refs.value
        .sortBy(ref => distance.apply(datum.feature, ref.feature)) //sort refs by distance
        .zipWithIndex //give rank (score)
        .map { case (ref, idx) => ReferencePointAssignment(datum.id, ref.id, idx) } //obj, postingListId, score
        .take(ki) //limit to number of nearest pivots used when indexing
    }).map(x => Row(x.refid, x.tid, x.score))*/


    val schema = StructType(Seq(
      StructField(MIIndex.REFERENCE_OBJ_NAME, IntegerType, nullable = false),
      StructField(MIIndex.POSTING_LIST_NAME, new ArrayType(entity.pk.fieldtype.datatype, false), nullable = false),
      StructField(MIIndex.SCORE_LIST_NAME, new ArrayType(IntegerType, false), nullable = false)
    ))

    val df = ac.sqlContext.createDataFrame(indexdata, schema)
    val meta = MIIndexMetaData(ki, ks, refs.value)

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