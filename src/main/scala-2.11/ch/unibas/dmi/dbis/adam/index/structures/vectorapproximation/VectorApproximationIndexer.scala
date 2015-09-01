package ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation

import ch.unibas.dmi.dbis.adam.data.IndexTuple
import ch.unibas.dmi.dbis.adam.data.types.Feature.{VectorBase, WorkingVector}
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.index.IndexGenerator
import ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.VectorApproximationIndexer.Marks
import ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.marks.{EquidistantMarksGenerator, EquifrequentMarksGenerator, MarksGenerator}
import ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.signature.{FixedSignatureGenerator, SignatureGenerator}
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.query.distance.Distance._
import ch.unibas.dmi.dbis.adam.table.Table._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.{Map => mMap}


/**
 * 
 */
class VectorApproximationIndexer(maxMarks: Int = 64, sampleSize: Int = 500, marksGenerator: MarksGenerator, signatureGenerator: SignatureGenerator) extends IndexGenerator with Serializable {
  override val indextypename: String = "va"

  protected val MAX_NUMBER_OF_SAMPLES = 2000

  /**
   * 
   */
  private def computeMarks(data: RDD[IndexTuple[WorkingVector]]): Marks = {
    marksGenerator.getMarks(data, maxMarks, sampleSize)
  }

  /**
   * 
   */
  override def index(indexname : IndexName, tablename : TableName, data: RDD[IndexTuple[WorkingVector]]): VectorApproximationIndex = {
    val marks = computeMarks(data)

    val indexdata = data.map(
      datum => {
        val cells = getCells(datum.value, marks)
        val signature = signatureGenerator.toSignature(cells)
        IndexTuple(datum.tid, signature)
      })


    import SparkStartup.sqlContext.implicits._
    new VectorApproximationIndex(indexname, tablename, indexdata.toDF, marks, signatureGenerator)
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
  type Marks = Seq[Seq[VectorBase]]
  type Bounds = Array[Array[Distance]]
  type Signature = Array[Byte]

  /**
   *
   * @param properties
   */
  def apply(properties : Map[String, String] = Map[String, String](), data: RDD[IndexTuple[WorkingVector]]) : IndexGenerator = {
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
      case "variable" => {
        //TODO
        null
      }
    }

    new VectorApproximationIndexer(maxMarks, sampleSize, marksGenerator, signatureGenerator)
  }
}