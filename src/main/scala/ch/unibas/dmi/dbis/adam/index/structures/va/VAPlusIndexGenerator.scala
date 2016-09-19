package ch.unibas.dmi.dbis.adam.index.structures.va

import breeze.linalg._
import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.datatypes.bitString.BitStringUDT
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.datatypes.feature.FeatureVectorWrapper
import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.exception.QueryNotConformException
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.index.structures.va.marks.VAPlusMarksGenerator
import ch.unibas.dmi.dbis.adam.index.structures.va.signature.VariableSignatureGenerator
import ch.unibas.dmi.dbis.adam.index.{IndexGeneratorFactory, IndexingTaskTuple, IndexGenerator}
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.distance.{DistanceFunction, MinkowskiDistance}
import org.apache.spark.mllib.feature.{PCA, PCAModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.util.random.Sampling

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
    * @param indexname  name of index
    * @param entityname name of entity
    * @param data       data to index
    * @return
    */
  override def index(indexname: IndexName, entityname: EntityName, data: RDD[IndexingTaskTuple[_]]): (DataFrame, Serializable) = {
    val entity = Entity.load(entityname).get

    val dims = ndims.getOrElse(data.first().feature.size)

    val sparkVecData = data.map(x => Vectors.dense(x.feature.toArray.map(_.toDouble)))

    val pca = new PCA(dims).fit(sparkVecData)
    val indexingdata = data
      .map(tuple => {
        val feature: FeatureVector = tuple.feature

        val sparkVector = Vectors.dense(feature.toArray.map(_.toDouble))
        val sparkTransformed = pca.transform(sparkVector)
        val transformedFVW = new FeatureVectorWrapper(sparkTransformed.toArray.map(_.toFloat))


        IndexingTaskTuple(tuple.id, transformedFVW.vector)
      })

    val n = entity.count
    val fraction = Sampling.computeFractionForSampleSize(math.max(trainingSize, MINIMUM_NUMBER_OF_TUPLE), n, withReplacement = false)
    var trainData = indexingdata.sample(false, fraction).collect()
    if (trainData.length < MINIMUM_NUMBER_OF_TUPLE) {
      trainData = indexingdata.take(MINIMUM_NUMBER_OF_TUPLE)
    }
    val meta = train(trainData.map(_.asInstanceOf[IndexingTaskTuple[_]]), pca, dims)

    log.debug("VA-File (plus) indexing...")

    val indexeddata = indexingdata.map(
      datum => {
        val cells = getCells(datum.feature, meta.marks)
        val signature = meta.signatureGenerator.toSignature(cells)
        Row(datum.id, signature)
      })

    val schema = StructType(Seq(
      StructField(entity.pk.name, entity.pk.fieldtype.datatype, false),
      StructField(FieldNames.featureIndexColumnName, new BitStringUDT, false)
    ))

    val df = ac.sqlContext.createDataFrame(indexeddata, schema)

    (df, meta)
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
  private def train(trainData: Array[IndexingTaskTuple[_]], pca: PCAModel, ndims : Int): VAPlusIndexMetaData = {
    log.trace("VA-File (variable) started training")

    //data
    val dTrainData = trainData.map(x => x.feature.map(x => x.toDouble).toArray)

    val dataMatrix = DenseMatrix(dTrainData.toList: _*)

    // pca
    val variance = diag(cov(dataMatrix, center = true)).toArray

    var k = 0
    var modes = Seq.fill(ndims)(0).toArray

    while (k < nbits.getOrElse(ndims * 8)) {
      val j = getMaxIndex(variance)
      modes(j) += 1
      variance(j) = variance(j) / 4.0
      k += 1
    }

    val signatureGenerator = new VariableSignatureGenerator(modes)
    val marks = VAPlusMarksGenerator.getMarks(trainData, modes.map(x => 2 << (x - 1)).toSeq)

    log.trace("VA-File (variable) finished training")

    new VAPlusIndexMetaData(marks, signatureGenerator, pca, ndims > pca.k)
  }


  /**
    *
    */
  @inline private def getCells(f: FeatureVector, marks: Seq[Seq[VectorBase]]): Seq[Int] = {
    f.toArray.zip(marks).map {
      case (x, l) =>
        val index = l.toArray.indexWhere(p => p >= x, 1)
        if (index == -1) l.length - 1 - 1 else index - 1
    }
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
}