package org.vitrivr.adampro.index.structures.mi

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.functions.udf
import org.vitrivr.adampro.config.AttributeNames
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

    val signatureGeneratorBc = ac.sc.broadcast(new MISignatureGenerator(ki, refs.length))
    tracker.addBroadcast(signatureGeneratorBc)

    val referencesUDF = udf((c: DenseSparkVector) => {
      val references = refsBc.value
        .sortBy(ref => distance.apply(Vector.conv_dspark2vec(c), ref.ap_indexable)) //sort refs by distance
        .take(ki)
        .map(x => (x.ap_id)) //refid

      signatureGeneratorBc.value.toSignature(references).serialize
    })

    val indexed = data.withColumn(AttributeNames.featureIndexColumnName, referencesUDF(data(attribute)))

    log.trace("MI finished indexing")
    val meta = MIIndexMetaData(ki, ks, refs)

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