package ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation

import ch.unibas.dmi.dbis.adam.data.types.Feature.{VectorBase, WorkingVector}
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.marks.{EquidistantMarksGenerator, EquifrequentMarksGenerator, MarksGenerator}
import ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.signature.{FixedSignatureGenerator, SignatureGenerator}
import ch.unibas.dmi.dbis.adam.index.{IndexerTuple, IndexGenerator, IndexTuple}
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.table.Table._
import org.apache.spark.adam.ADAMSamplingUtils
import org.apache.spark.rdd.RDD

import scala.collection.mutable.{Map => mMap}


/**
 * 
 */
class VectorApproximationIndexer(maxMarks: Int = 64, sampleSize: Int = 500, marksGenerator: MarksGenerator, signatureGenerator: SignatureGenerator, trainingSize : Int) extends IndexGenerator with Serializable {
  override val indextypename: String = "va"

  /**
   * 
   */
  override def index(indexname : IndexName, tablename : TableName, data: RDD[IndexerTuple[WorkingVector]]): VectorApproximationIndex = {
    val indexMetaData = train(data)

    val indexdata = data.map(
      datum => {
        val cells = getCells(datum.value, indexMetaData.marks)
        val signature = signatureGenerator.toSignature(cells)
        IndexTuple(datum.tid, signature)
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
    val fraction = ADAMSamplingUtils.computeFractionForSampleSize(sampleSize, data.count(), false)
    val trainData = data.sample(false, fraction)

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
    val sampleSize = properties.getOrElse("sampleSize", "500").toInt

    val marksGeneratorDescription = properties.getOrElse("marksGenerator", "equidistant")
    val marksGenerator = marksGeneratorDescription.toLowerCase match {
      case "equifrequent" => EquifrequentMarksGenerator
      case "equidistant" => EquidistantMarksGenerator
    }

    val signatureGeneratorDescription = properties.getOrElse("signatureGenerator", "fixed")
    val fixedNumBits = properties.getOrElse("fixedNumBits", "8").toInt

    val signatureGenerator = signatureGeneratorDescription.toLowerCase match {
      case "fixed" => {
        val dim = data.first.value.length
        val bitsPerDim = fixedNumBits
        new FixedSignatureGenerator(dim, bitsPerDim)
      }
    }

    val trainingSize = properties.getOrElse("trainingSize", "50000").toInt


    new VectorApproximationIndexer(maxMarks, sampleSize, marksGenerator, signatureGenerator, trainingSize)
  }
}