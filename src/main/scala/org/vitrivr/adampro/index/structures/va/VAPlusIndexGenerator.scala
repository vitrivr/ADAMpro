package org.vitrivr.adampro.index.structures.va

import breeze.linalg._
import org.apache.spark.mllib.feature.{PCA, PCAModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.vitrivr.adampro.config.AttributeNames
import org.vitrivr.adampro.datatypes.vector.Vector._
import org.vitrivr.adampro.exception.QueryNotConformException
import org.vitrivr.adampro.helpers.tracker.OperationTracker
import org.vitrivr.adampro.index.Index._
import org.vitrivr.adampro.index.structures.IndexTypes
import org.vitrivr.adampro.index.structures.va.marks.VAPlusMarksGenerator
import org.vitrivr.adampro.index.structures.va.signature.VariableSignatureGenerator
import org.vitrivr.adampro.index.{IndexGenerator, IndexGeneratorFactory, IndexingTaskTuple, ParameterInfo}
import org.vitrivr.adampro.main.AdamContext
import org.vitrivr.adampro.query.distance.{DistanceFunction, MinkowskiDistance}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * September 2016
  *
  * see H. Ferhatosmanoglu, E. Tuncel, D. Agrawal, A. El Abbadi (2006): High dimensional nearest neighbor searching. Information Systems.
  */
class VAPlusIndexGenerator(nbits: Option[Int], ndims : Option[Int], trainingSize: Int, distance: MinkowskiDistance)(@transient implicit val ac: AdamContext) extends IndexGenerator {
  override val indextypename: IndexTypeName = IndexTypes.VAPLUSINDEX

  /**
    *
    * @param data raw data to index
    * @return
    */
  override def index(data: DataFrame, attribute : String)(tracker : OperationTracker): (DataFrame, Serializable) = {
    log.trace("VA-File (plus) started indexing")

    val meta = train(getSample(math.max(trainingSize, MINIMUM_NUMBER_OF_TUPLE), attribute)(data), data, attribute)

    val cellUDF = udf((c: DenseSparkVector) => {
      val cells = getCells(meta.pca.transform(Vectors.dense(c.toArray.map(_.toDouble))).toArray, meta.marks)
      meta.signatureGenerator.toSignature(cells).serialize
    })
    val indexed = data.withColumn(AttributeNames.featureIndexColumnName, cellUDF(data(attribute)))

    log.trace("VA-File (plus) finished indexing")

    (indexed, meta)
  }


  /**
    *
    * @param array
    * @return
    */
  private def getMaxIndex(array: Array[Double]): Int = {
    var maxIndex = -1
    var max = Double.MinValue
    for (index <- 0 until array.length) {
      val element = array(index)
      if (element > max) {
        max = element
        maxIndex = index
      }
    }
    maxIndex
  }

  /**
    *
    * @param trainData training data
    * @return
    */
  private def train(trainData: Seq[IndexingTaskTuple], data : DataFrame, attribute : String): VAPlusIndexMetaData = {
    log.trace("VA-File (variable) started training")
    val dims = ndims.getOrElse(trainData.head.ap_indexable.size)

    import ac.spark.implicits._
    val pca = new PCA(dims).fit(data.rdd.map(x => Vectors.dense(x.getAs[DenseSparkVector](attribute).toArray)))


    //data
    val dTrainData = trainData.map(x => x.ap_indexable.map(x => x.toDouble).toArray)

    val dataMatrix = DenseMatrix(dTrainData.toList: _*)

    // pca
    val variance = diag(cov(dataMatrix, center = true)).toArray

    var k = 0
    var modes = Seq.fill(dims)(0).toArray

    while (k < nbits.getOrElse(dims * 8)) {
      val j = getMaxIndex(variance)
      modes(j) += 1
      variance(j) = variance(j) / 4.0
      k += 1
    }

    val signatureGenerator = new VariableSignatureGenerator(modes)

    val marks = VAPlusMarksGenerator.getMarks(trainData, modes.map(x => math.max(1, 2 << (x - 1))).toSeq)

    log.trace("VA-File (variable) finished training")

    new VAPlusIndexMetaData(marks, signatureGenerator, pca, dims > pca.k)
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


class VAPlusIndexGeneratorFactory extends IndexGeneratorFactory {
  /**
    * @param distance   distance function
    * @param properties indexing properties
    */
  def getIndexGenerator(distance: DistanceFunction, properties: Map[String, String] = Map[String, String]())(implicit ac: AdamContext): IndexGenerator = {
    if (!distance.isInstanceOf[MinkowskiDistance]) {
      log.error("only Minkowski distances allowed for VAV Indexer")
      throw new QueryNotConformException()
    }

    val nbits = if (properties.get("signature-nbits").isDefined) {
      Some(properties.get("signature-nbits").get.toInt)
    } else {
      None
    }
    val trainingSize = properties.getOrElse("ntraining", "1000").toInt

    val ndims = properties.get("ndims").map(_.toInt)


    new VAPlusIndexGenerator(nbits, ndims, trainingSize, distance.asInstanceOf[MinkowskiDistance])
  }

  /**
    *
    * @return
    */
  override def parametersInfo: Seq[ParameterInfo] = Seq(
    new ParameterInfo("ntraining", "number of training tuples", Seq[String]()),
    new ParameterInfo("signature-nbits", "number of bits for the complete signature", Seq(32, 64, 128, 256, 1024).map(_.toString)),
    new ParameterInfo("ndims", "distribution of marks", Seq(64, 128, 256, 512, 1024).map(_.toString)) //TODO: this should rather be a function based on the ndims
  )
}