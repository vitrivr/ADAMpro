package org.vitrivr.adampro.data.index.structures.va

import breeze.linalg._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.vitrivr.adampro.config.AttributeNames
import org.vitrivr.adampro.data.datatypes.vector.Vector._
import org.vitrivr.adampro.utils.exception.QueryNotConformException
import org.vitrivr.adampro.query.tracker.QueryTracker
import org.vitrivr.adampro.data.index.Index.IndexTypeName
import org.vitrivr.adampro.data.index.structures.IndexTypes
import org.vitrivr.adampro.data.index.structures.va.marks.{EquidistantMarksGenerator, EquifrequentMarksGenerator, MarksGenerator}
import org.vitrivr.adampro.data.index.structures.va.signature.VariableSignatureGenerator
import org.vitrivr.adampro.data.index.{IndexGenerator, IndexGeneratorFactory, IndexingTaskTuple, ParameterInfo}
import org.vitrivr.adampro.process.SharedComponentContext
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
class VAVIndexGenerator(totalNumOfBits: Option[Int], fixedNumBitsPerDimension : Option[Int], marksGenerator: MarksGenerator, trainingSize: Int, distance: MinkowskiDistance)(@transient implicit val ac: SharedComponentContext) extends IndexGenerator {
  override val indextypename: IndexTypeName = IndexTypes.VAVINDEX

  assert(!(totalNumOfBits.isDefined && fixedNumBitsPerDimension.isDefined))


  /**
    *
    * @param data raw data to index
    * @return
    */
  override def index(data: DataFrame, attribute : String)(tracker : QueryTracker): (DataFrame, Serializable) = {
    log.trace("VA-File (variable) started indexing")

    val meta = train(getSample(math.max(trainingSize, MINIMUM_NUMBER_OF_TUPLE), attribute)(data))

    val cellUDF = udf((c: DenseSparkVector) => {
      getCells(c, meta.marks).map(_.toShort)
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
    val doubleTrainData = trainData.map(x => x.ap_indexable.map(x => x.toDouble).toArray)

    val dataMatrix = DenseMatrix(doubleTrainData.toList: _*)

    val ndims = doubleTrainData.head.length
    val nbits = math.max(ndims, totalNumOfBits.getOrElse(ndims * fixedNumBitsPerDimension.getOrElse(5)))

    // pca
    val variance = diag(cov(dataMatrix, center = true)).toArray
    val sumVariance = variance.sum

    // compute shares of bits
    val bitsPerDim = variance.map(variance => 1 + (variance / sumVariance * (nbits - ndims)).toInt)

    val signatureGenerator = new VariableSignatureGenerator(bitsPerDim)
    val marks = marksGenerator.getMarks(trainData, bitsPerDim.map(x => 1 << x))

    log.trace("VA-File (variable) finished training")

    VAIndexMetaData(marks, signatureGenerator)
  }


  /**
    *
    */
  @inline private def getCells(f: Iterable[VectorBase], marks: Seq[Seq[VectorBase]]): Seq[Int] = {
    f.zip(marks).map {
      case (x, l) =>
        val index = l.toArray.indexWhere(p => p >= x, 1)
        if (index == -1) l.length - 1 - 1 else index - 1
    }.toSeq
  }
}


class VAVIndexGeneratorFactory extends IndexGeneratorFactory {
  /**
    * @param distance   distance function
    * @param properties indexing properties
    */
  def getIndexGenerator(distance: DistanceFunction, properties: Map[String, String] = Map[String, String]())(implicit ac: SharedComponentContext): IndexGenerator = {
    if (!distance.isInstanceOf[MinkowskiDistance]) {
      throw new QueryNotConformException("VAF index only supports Minkowski distance")
    }

    val marksGeneratorDescription = properties.getOrElse("marktype", "equifrequent")
    val marksGenerator = marksGeneratorDescription.toLowerCase match {
      case "equifrequent" => EquifrequentMarksGenerator
      case "equidistant" => EquidistantMarksGenerator
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