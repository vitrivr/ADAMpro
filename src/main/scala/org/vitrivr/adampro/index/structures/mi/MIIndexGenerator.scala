package org.vitrivr.adampro.index.structures.mi

import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{ArrayType, IntegerType, StructField, StructType}
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
@Experimental class MIIndexGenerator(p_ki: Option[Int], p_ks: Option[Int], distance: DistanceFunction, nrefs: Int)(@transient implicit val ac: AdamContext) extends IndexGenerator {
  override val indextypename: IndexTypeName = IndexTypes.MIINDEX

  /**
    *
    * @param data raw data to index
    * @return
    */
  override def index(data: DataFrame, attribute: String)(tracker: OperationTracker): (DataFrame, Serializable) = {
    log.trace("LSH started indexing")

    val sample = getSample(math.max(nrefs, MINIMUM_NUMBER_OF_TUPLE), attribute)(data)

    val refs = sample.zipWithIndex.map { case (idt, idx) => IndexingTaskTuple(idx.toLong, idt.ap_indexable) }
    val refsBc = ac.sc.broadcast(refs)
    tracker.addBroadcast(refsBc)

    val ki = p_ki.getOrElse(math.min(100, refsBc.value.length))
    val ks = p_ks.getOrElse(math.min(100, refsBc.value.length))
    assert(ks <= ki)
    log.trace("MI index chosen " + refsBc.value.length + " reference points")


    val referencesUDF = udf((c: DenseSparkVector) => {
      refsBc.value
        .sortBy(ref => distance.apply(Vector.conv_dspark2vec(c), ref.ap_indexable)) //sort refs by distance
        .zipWithIndex //give rank (score)
        .take(ki)
        .map(x => (x._1.ap_id, x._2)) //refid, score
    })

    import org.apache.spark.sql.functions.{col, explode}

    val rdd = data.withColumn(AttributeNames.featureIndexColumnName, referencesUDF(data(attribute)))
      .withColumn(AttributeNames.featureIndexColumnName, explode(col(AttributeNames.featureIndexColumnName)))
      .select(col(AttributeNames.internalIdColumnName),
        col(AttributeNames.featureIndexColumnName).getField("_1") as AttributeNames.featureIndexColumnName + "-id", //refid
        col(AttributeNames.featureIndexColumnName).getField("_2") as AttributeNames.featureIndexColumnName + "-score" //score
      ).rdd
      .groupBy(_.getAs[TupleID](AttributeNames.featureIndexColumnName + "-id"))
      .mapValues { vals =>
        val list = vals.toList
          .sortBy(_.getAs[Int](AttributeNames.featureIndexColumnName + "-score")) //order by score
        (list.map(_.getAs[TupleID](AttributeNames.internalIdColumnName)), list.map(_.getAs[Int](AttributeNames.featureIndexColumnName + "-score")))
      }.map(x => Row(x._1, x._2._1, x._2._2))


    val schema = StructType(Seq(
      StructField(MIIndex.REFERENCE_OBJ_NAME, TupleID.SparkTupleID, nullable = false),
      StructField(MIIndex.POSTING_LIST_NAME, new ArrayType(data.schema.fields.apply(0).dataType, false), nullable = false),
      StructField(MIIndex.SCORE_LIST_NAME, new ArrayType(IntegerType, false), nullable = false)
    ))

    val indexed = ac.sqlContext.createDataFrame(rdd, schema)


    log.trace("MI finished indexing")
    val meta = MIIndexMetaData(ki, ks, refsBc.value)

    (indexed, meta)
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

    val nrefs = if (properties.contains("nrefs")) {
      properties.get("nrefs").get.toInt
    } else {
      if (properties.contains("n")) {
        math.ceil(2 * math.sqrt(properties.get("n").get.toInt)).toInt
      } else {
        6000 //assuming we have around 10M elements
      }
    }

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