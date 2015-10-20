package ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation

import breeze.linalg._
import ch.unibas.dmi.dbis.adam.datatypes.Feature._
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.index.structures.IndexStructures
import ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.marks.{EquidistantMarksGenerator, EquifrequentMarksGenerator, MarksGenerator}
import ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.signature.VariableSignatureGenerator
import ch.unibas.dmi.dbis.adam.index.{IndexerTuple, IndexGenerator, BitStringIndexTuple}
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.table.Table._
import org.apache.spark.adam.ADAMSamplingUtils
import org.apache.spark.rdd.RDD

/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
class NewVectorApproximationIndexer (nbits : Int, marksGenerator: MarksGenerator, trainingSize : Int) extends IndexGenerator with Serializable {
  override val indextypename: IndexTypeName = IndexStructures.VAV

  /**
   *
   */
  override def index(indexname : IndexName, tablename : TableName, data: RDD[IndexerTuple[WorkingVector]]): VectorApproximationIndex = {
    val indexMetaData = train(data)

    val indexdata = data.map(
      datum => {
        val cells = getCells(datum.value, indexMetaData.marks)
        val signature = indexMetaData.signatureGenerator.toSignature(cells)
        BitStringIndexTuple(datum.tid, signature)
      })

    import SparkStartup.sqlContext.implicits._
    new VectorApproximationIndex(indexname, tablename, indexdata.toDF, indexMetaData)
  }

  /**
   *
   * @param data
   * @return
   */
  private def train(data : RDD[IndexerTuple[WorkingVector]]) : VectorApproximationIndexMetaData = {
    //data
    val fraction = ADAMSamplingUtils.computeFractionForSampleSize(trainingSize, data.count(), false)
    val trainData = data.sample(false, fraction)
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

    VectorApproximationIndexMetaData(marks, signatureGenerator)
  }


  /**
   *
   */
  @inline private def getCells(f: WorkingVector, marks: Seq[Seq[VectorBase]]): Seq[Int] = {
    f.toArray.zip(marks).map {
      case (x, l) =>
        val index = l.toArray.indexWhere(p => p >= x, 1)
        if (index == -1) l.length - 1 - 1 else index - 1
    }
  }
}

object NewVectorApproximationIndexer {
  /**
   *
   * @param properties
   */
  def apply(properties : Map[String, String] = Map[String, String](), data: RDD[IndexerTuple[WorkingVector]]) : IndexGenerator = {
    val maxMarks = properties.getOrElse("maxMarks", "64").toInt

    val marksGeneratorDescription = properties.getOrElse("marksGenerator", "equifrequent")
    val marksGenerator = marksGeneratorDescription.toLowerCase match {
      case "equifrequent" => EquifrequentMarksGenerator
      case "equidistant" => EquidistantMarksGenerator
    }

    val signatureGeneratorDescription = properties.getOrElse("signatureGenerator", "variable")
    val totalNumBits = properties.getOrElse("totalNumBits", (data.first.value.length * 8).toString).toInt

    val trainingSize = properties.getOrElse("trainingSize", "5000").toInt


    new NewVectorApproximationIndexer(totalNumBits, marksGenerator, trainingSize)
  }
}