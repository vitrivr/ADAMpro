package org.vitrivr.adampro.data.index.structures.va

import breeze.linalg._
import org.apache.spark.ml.feature.PCA
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.vitrivr.adampro.config.AttributeNames
import org.apache.spark.ml.linalg.{DenseVector, Vectors}
import org.vitrivr.adampro.data.datatypes.vector.Vector._
import org.vitrivr.adampro.utils.exception.QueryNotConformException
import org.vitrivr.adampro.query.tracker.QueryTracker
import org.vitrivr.adampro.data.index.Index._
import org.vitrivr.adampro.data.index.structures.IndexTypes
import org.vitrivr.adampro.data.index.structures.va.marks.VAPlusMarksGenerator
import org.vitrivr.adampro.data.index.structures.va.signature.VariableSignatureGenerator
import org.vitrivr.adampro.data.index.{IndexGenerator, IndexGeneratorFactory, IndexingTaskTuple, ParameterInfo}
import org.vitrivr.adampro.process.SharedComponentContext
import org.vitrivr.adampro.query.distance.{DistanceFunction, MinkowskiDistance}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * September 2016
  *
  * see H. Ferhatosmanoglu, E. Tuncel, D. Agrawal, A. El Abbadi (2006): High dimensional nearest neighbor searching. Information Systems.
  */
class VAPlusIndexGenerator(totalNumOfBits: Option[Int], ndims : Option[Int], trainingSize: Int, distance: MinkowskiDistance)(@transient implicit val ac: SharedComponentContext) extends IndexGenerator {
  override val indextypename: IndexTypeName = IndexTypes.VAPLUSINDEX

  /**
    *
    * @param data raw data to index
    * @return
    */
  override def index(data: DataFrame, attribute : String)(tracker : QueryTracker): (DataFrame, Serializable) = {
    log.trace("VA-File (plus) started indexing")

    val meta = train(getSample(math.max(trainingSize, MINIMUM_NUMBER_OF_TUPLE), attribute)(data), data, attribute)

    val pcaBc = ac.sc.broadcast(meta.pca)

    val cellUDF = udf((c: DenseVector) => {
      getCells(c.values, meta.marks).map(_.toShort)
    })

    val transformed = pcaBc.value.setInputCol(attribute).setOutputCol("ap_" + attribute + "pca").transform(data.withColumn(attribute, toVecUDF(data(attribute)))).drop(attribute).withColumnRenamed("ap_" + attribute + "pca", attribute)
    val indexed = transformed.withColumn(AttributeNames.featureIndexColumnName, cellUDF(transformed(attribute)))

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
    log.trace("VA-File (plus) started training")
    val dim = ndims.getOrElse(trainData.head.ap_indexable.size)

    val pca = new PCA().setInputCol(attribute + "_vec").setK(dim).fit(data.withColumn(attribute + "_vec", toVecUDF(data(attribute))))

    //data
    val dTrainData = trainData.map(x => x.ap_indexable.map(x => x.toDouble).toArray)

    val dataMatrix = DenseMatrix(dTrainData.toList: _*)

    // pca
    val variance = diag(cov(dataMatrix, center = true)).toArray

    var k = 0
    var modes = Seq.fill(dim)(0).toArray

    //based on results from paper and from Weber/Böhm (2000): Trading Quality for Time with Nearest Neighbor Search
    val nbits = totalNumOfBits.getOrElse(dim * math.max(5, math.ceil(5 + 0.5 * math.log(dim / 10) / math.log(2)).toInt))

    while (k < nbits) {
      val j = getMaxIndex(variance)
      modes(j) += 1
      variance(j) = variance(j) / 4.0
      k += 1
    }

    val signatureGenerator = new VariableSignatureGenerator(modes)

    val marks = VAPlusMarksGenerator.getMarks(trainData, modes.map(x => math.max(1, 2 << (x - 1))).toSeq)

    log.trace("VA-File (variable) finished training")

    new VAPlusIndexMetaData(marks, signatureGenerator, pca, dim > pca.getK)
  }


  val toVecUDF = udf((c: DenseSparkVector) => {
    Vectors.dense(c.map(_.toDouble).toArray)
  })


  /**
    *
    */
  @inline private def getCells(f: Iterable[Double], marks: Seq[Seq[VectorBase]]): Seq[Int] = {
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
  def getIndexGenerator(distance: DistanceFunction, properties: Map[String, String] = Map[String, String]())(implicit ac: SharedComponentContext): IndexGenerator = {
    if (!distance.isInstanceOf[MinkowskiDistance]) {
      throw new QueryNotConformException("VAF index only supports Minkowski distance")
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