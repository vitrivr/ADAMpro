package ch.unibas.dmi.dbis.adam.index.structures.va

import breeze.linalg._
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.index.structures.va.marks.{EquidistantMarksGenerator, EquifrequentMarksGenerator, MarksGenerator}
import ch.unibas.dmi.dbis.adam.index.structures.va.signature.VariableSignatureGenerator
import ch.unibas.dmi.dbis.adam.index.{BitStringIndexTuple, IndexGenerator, IndexingTaskTuple}
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.query.distance.MinkowskiDistance
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.util.random.ADAMSamplingUtils

/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
class VAVIndexer (nbits : Int, marksGenerator: MarksGenerator, trainingSize : Int, distance : MinkowskiDistance) extends IndexGenerator with Serializable {
  @transient lazy val log = Logger.getLogger(getClass.getName)

  override val indextypename: IndexTypeName = IndexTypes.VAVINDEX

  /**
   *
   */
  override def index(indexname : IndexName, entityname : EntityName, data: RDD[IndexingTaskTuple]): VAIndex = {
    val n = Entity.countTuples(entityname)
    val fraction = ADAMSamplingUtils.computeFractionForSampleSize(trainingSize, n, false)
    val trainData = data.sample(false, fraction)

    val indexMetaData = train(trainData.collect())

    log.debug("VA-File (variable) indexing...")

    val indexdata = data.map(
      datum => {
        val cells = getCells(datum.feature, indexMetaData.marks)
        val signature = indexMetaData.signatureGenerator.toSignature(cells)
        BitStringIndexTuple(datum.id, signature)
      })

    import SparkStartup.sqlContext.implicits._
    new VAIndex(indexname, entityname, indexdata.toDF, indexMetaData)
  }

  /**
   *
   * @param trainData
   * @return
   */
  private def train(trainData : Array[IndexingTaskTuple]) : VAIndexMetaData = {
    log.debug("VA-File (variable) started training")

    //data
    val dTrainData = trainData.map(x => x.feature.map(x => x.toDouble).toArray)

    val dataMatrix = DenseMatrix(dTrainData.toList : _*)

    val nfeatures =  dTrainData.head.length
    val numComponents = math.min(nfeatures, nbits)

    // pca
    val variance = diag(cov(dataMatrix, true)).toArray
    val sumVariance = variance.sum

    // compute shares of bits
    val modes = variance.map(variance => (variance / sumVariance * nbits).toInt)

    val signatureGenerator =  new VariableSignatureGenerator(modes)
    val marks = marksGenerator.getMarks(trainData, modes.map(x => 2 << (x - 1)))

    log.debug("VA-File (variable) finished training")

    VAIndexMetaData(marks, signatureGenerator, distance)
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


object VAVIndexer {
  /**
   *
   * @param properties
   */
  def apply(dimensions : Int, distance : MinkowskiDistance, properties : Map[String, String] = Map[String, String]()) : IndexGenerator = {
    val marksGeneratorDescription = properties.getOrElse("marktype", "equifrequent")
    val marksGenerator = marksGeneratorDescription.toLowerCase match {
      case "equifrequent" => EquifrequentMarksGenerator
      case "equidistant" => EquidistantMarksGenerator
    }

    val totalNumBits = properties.getOrElse("signature-nbits", (dimensions * 8).toString).toInt
    val trainingSize = properties.getOrElse("ntraining", "1000").toInt

    new VAVIndexer(totalNumBits, marksGenerator, trainingSize, distance)
  }
}