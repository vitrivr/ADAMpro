package org.vitrivr.adampro.index.structures.va

import breeze.linalg._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.vitrivr.adampro.config.AttributeNames
import org.vitrivr.adampro.datatypes.vector.Vector._
import org.vitrivr.adampro.exception.QueryNotConformException
import org.vitrivr.adampro.index.Index.IndexTypeName
import org.vitrivr.adampro.index.structures.IndexTypes
import org.vitrivr.adampro.index.structures.va.marks.{EquidistantMarksGenerator, EquifrequentMarksGenerator, MarksGenerator}
import org.vitrivr.adampro.index.structures.va.signature.VariableSignatureGenerator
import org.vitrivr.adampro.index.{IndexGenerator, IndexGeneratorFactory, IndexingTaskTuple, ParameterInfo}
import org.vitrivr.adampro.main.AdamContext
import org.vitrivr.adampro.query.distance.{DistanceFunction, MinkowskiDistance}

/**
  * adamtwo
  *
  * Ivan Giangreco
  * September 2015
  *
  * VAV: this VA-File index will have a training phase in which we learn the number of bits per dimension (new version of VA-File)
  * note that using VAF, we may still use both the equidistant or the equifrequent marks generator
  */
class VAVIndexGenerator(nbits_total: Option[Int], nbits_dim : Option[Int], marksGenerator: MarksGenerator, trainingSize: Int, distance: MinkowskiDistance)(@transient implicit val ac: AdamContext) extends IndexGenerator {
  override val indextypename: IndexTypeName = IndexTypes.VAVINDEX

  assert(!(nbits_total.isDefined && nbits_dim.isDefined))


  /**
    *
    * @param data raw data to index
    * @return
    */
  override def index(data: DataFrame, attribute : String): (DataFrame, Serializable) = {
    log.trace("VA-File (variable) started indexing")

    val meta = train(getSample(math.max(trainingSize, MINIMUM_NUMBER_OF_TUPLE), attribute)(data))

    val cellUDF = udf((c: DenseSparkVector) => {
      val cells = getCells(c, meta.marks)
      meta.signatureGenerator.toSignature(cells).serialize
    })
    val indexed = data.withColumn(AttributeNames.featureIndexColumnName, cellUDF(data(attribute)))

    log.trace("VA-File (variable) finished indexing")

    (indexed, meta)
  }

  /**
    *
    * @param trainData training data
    * @return
    */
  private def train(trainData: Seq[IndexingTaskTuple]): VAIndexMetaData = {
    log.trace("VA-File (variable) started training")

    //data
    val dTrainData = trainData.map(x => x.ap_indexable.map(x => x.toDouble).toArray)

    val dataMatrix = DenseMatrix(dTrainData.toList: _*)

    val nfeatures = dTrainData.head.length
    val numComponents = math.min(nfeatures, nbits_total.getOrElse(nfeatures * nbits_dim.getOrElse(8)))

    // pca
    val variance = diag(cov(dataMatrix, center = true)).toArray
    val sumVariance = variance.sum

    // compute shares of bits
    val modes = variance.map(variance => (variance / sumVariance * nbits_total.getOrElse(nfeatures * nbits_dim.getOrElse(8))).toInt)

    val signatureGenerator = new VariableSignatureGenerator(modes)
    val marks = marksGenerator.getMarks(trainData, modes.map(x => 2 << (x - 1)))

    log.trace("VA-File (variable) finished training")

    VAIndexMetaData(marks, signatureGenerator)
  }


  /**
    *
    */
  @inline private def getCells(f: Iterable[VectorBase], marks: Seq[Seq[VectorBase]]): Seq[Int] = {
    f.toArray.zip(marks).map {
      case (x, l) =>
        val index = l.toArray.indexWhere(p => p >= x, 1)
        if (index == -1) l.length - 1 - 1 else index - 1
    }
  }
}


class VAVIndexGeneratorFactory extends IndexGeneratorFactory {
  /**
    * @param distance   distance function
    * @param properties indexing properties
    */
  def getIndexGenerator(distance: DistanceFunction, properties: Map[String, String] = Map[String, String]())(implicit ac: AdamContext): IndexGenerator = {
    val marksGeneratorDescription = properties.getOrElse("marktype", "equifrequent")
    val marksGenerator = marksGeneratorDescription.toLowerCase match {
      case "equifrequent" => EquifrequentMarksGenerator
      case "equidistant" => EquidistantMarksGenerator
    }

    if (!distance.isInstanceOf[MinkowskiDistance]) {
      log.error("only Minkowski distances allowed for VAV Indexer")
      throw new QueryNotConformException()
    }

    val totalNumBits = if (properties.get("signature-nbits").isDefined) {
      Some(properties.get("signature-nbits").get.toInt)
    } else {
      None
    }

    val bitsPerDim = if (properties.get("signature-nbits-dim").isDefined) {
      Some(properties.get("signature-nbits-dim").get.toInt)
    } else {
      None
    }

    val trainingSize = properties.getOrElse("ntraining", "1000").toInt

    new VAVIndexGenerator(totalNumBits, bitsPerDim, marksGenerator, trainingSize, distance.asInstanceOf[MinkowskiDistance])
  }


  /**
    *
    * @return
    */
  override def parametersInfo: Seq[ParameterInfo] = Seq(
    new ParameterInfo("ntraining", "number of training tuples", Seq[String]()),
    new ParameterInfo("signature-nbits", "number of bits for the complete signature", Seq()),
    new ParameterInfo("signature-nbits-dim", "number of bits for signature, given per dim", Seq(5, 8, 10, 12).map(_.toString)),
    new ParameterInfo("marktype", "distribution of marks", Seq("equidistant", "equifrequent"))
  )
}