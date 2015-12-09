package ch.unibas.dmi.dbis.adam.index.structures.va

import breeze.linalg._
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.index.structures.IndexStructures
import ch.unibas.dmi.dbis.adam.index.structures.va.marks.{EquidistantMarksGenerator, EquifrequentMarksGenerator, MarksGenerator}
import ch.unibas.dmi.dbis.adam.index.structures.va.signature.VariableSignatureGenerator
import ch.unibas.dmi.dbis.adam.index.{BitStringIndexTuple, IndexGenerator, IndexerTuple}
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.query.distance.MinkowskiDistance
import org.apache.spark.rdd.RDD
import org.apache.spark.util.random.ADAMSamplingUtils

/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
class VAVIndexer (nbits : Int, marksGenerator: MarksGenerator, trainingSize : Int, distance : MinkowskiDistance) extends IndexGenerator with Serializable {
  override val indextypename: IndexTypeName = IndexStructures.VAV

  /**
   *
   */
  override def index(indexname : IndexName, entityname : EntityName, data: RDD[IndexerTuple]): VAIndex = {
    val n = Entity.countEntity(entityname)
    val fraction = ADAMSamplingUtils.computeFractionForSampleSize(trainingSize, n, false)
    val trainData = data.sample(false, fraction)

    val indexMetaData = train(trainData)

    val indexdata = data.map(
      datum => {
        val cells = getCells(datum.value, indexMetaData.marks)
        val signature = indexMetaData.signatureGenerator.toSignature(cells)
        BitStringIndexTuple(datum.tid, signature)
      })

    import SparkStartup.sqlContext.implicits._
    new VAIndex(indexname, entityname, indexdata.toDF, indexMetaData)
  }

  /**
   *
   * @param trainData
   * @return
   */
  private def train(trainData : RDD[IndexerTuple]) : VAIndexMetaData = {
    //data
    val dTrainData = trainData.map(x => x.value.map(x => x.toDouble).toArray)

    val dataMatrix = DenseMatrix(dTrainData.collect.toList : _*)

    val nfeatures =  dTrainData.first.length
    val numComponents = math.min(nfeatures, nbits)

    // pca
    val variance = diag(cov(dataMatrix, true)).toArray
    val sumVariance = variance.sum

    // compute shares of bits
    val modes = variance.map(variance => (variance / sumVariance * nbits).toInt)

    val signatureGenerator =  new VariableSignatureGenerator(modes)
    val marks = marksGenerator.getMarks(trainData, modes.map(x => 2 << (x - 1)))

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
  def apply(properties : Map[String, String] = Map[String, String](), distance : MinkowskiDistance, data: RDD[IndexerTuple]) : IndexGenerator = {
    val maxMarks = properties.getOrElse("maxMarks", "64").toInt

    val marksGeneratorDescription = properties.getOrElse("marksGenerator", "equifrequent")
    val marksGenerator = marksGeneratorDescription.toLowerCase match {
      case "equifrequent" => EquifrequentMarksGenerator
      case "equidistant" => EquidistantMarksGenerator
    }

    val signatureGeneratorDescription = properties.getOrElse("signatureGenerator", "variable")
    val totalNumBits = properties.getOrElse("totalNumBits", (data.first.value.length * 8).toString).toInt

    val trainingSize = properties.getOrElse("trainingSize", "5000").toInt


    new VAVIndexer(totalNumBits, marksGenerator, trainingSize, distance)
  }
}