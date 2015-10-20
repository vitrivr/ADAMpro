package ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation

import ch.unibas.dmi.dbis.adam.datatypes.Feature.{VectorBase, WorkingVector}
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.index.structures.IndexStructures
import ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.marks.{EquidistantMarksGenerator, EquifrequentMarksGenerator, MarksGenerator}
import ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.signature.FixedSignatureGenerator
import ch.unibas.dmi.dbis.adam.index.{IndexerTuple, IndexGenerator, BitStringIndexTuple}
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.table.Table._
import org.apache.spark.adam.ADAMSamplingUtils
import org.apache.spark.rdd.RDD


/**
 * 
 */
class VectorApproximationIndexer(maxMarks: Int = 64, marksGenerator: MarksGenerator, bitsPerDimension : Int, trainingSize : Int) extends IndexGenerator with Serializable {
  override val indextypename: IndexTypeName = IndexStructures.VAF

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

    val dim = trainData.first.value.length

    val signatureGenerator =  new FixedSignatureGenerator(dim, bitsPerDimension)
    val marks = marksGenerator.getMarks(trainData, maxMarks)

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

object VectorApproximationIndexer {
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

    val signatureGeneratorDescription = properties.getOrElse("signatureGenerator", "fixed")
    val fixedNumBitsPerDimension = properties.getOrElse("fixedNumBits", "8").toInt

    val trainingSize = properties.getOrElse("trainingSize", "5000").toInt


    new VectorApproximationIndexer(maxMarks, marksGenerator, fixedNumBitsPerDimension, trainingSize)
  }
}