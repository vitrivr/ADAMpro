package ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation

import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature
import Feature.{VectorBase, FeatureVector}
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.index.structures.IndexStructures
import ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.marks.{EquidistantMarksGenerator, EquifrequentMarksGenerator, MarksGenerator}
import ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.signature.FixedSignatureGenerator
import ch.unibas.dmi.dbis.adam.index.{IndexerTuple, IndexGenerator, BitStringIndexTuple}
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.query.distance.MinkowskiDistance
import ch.unibas.dmi.dbis.adam.entity.Entity._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.random.ADAMSamplingUtils


/**
 * 
 */
class VectorApproximationIndexer(maxMarks: Int = 64, marksGenerator: MarksGenerator, bitsPerDimension : Int, trainingSize : Int, distance : MinkowskiDistance) extends IndexGenerator with Serializable {
  override val indextypename: IndexTypeName = IndexStructures.VAF

  /**
   * 
   */
  override def index(indexname : IndexName, entityname : EntityName, data: RDD[IndexerTuple]): VectorApproximationIndex = {
    val indexMetaData = train(data)

    val indexdata = data.map(
      datum => {
        val cells = getCells(datum.value, indexMetaData.marks)
        val signature = indexMetaData.signatureGenerator.toSignature(cells)
        BitStringIndexTuple(datum.tid, signature)
      })

    import SparkStartup.sqlContext.implicits._
    new VectorApproximationIndex(indexname, entityname, indexdata.toDF, indexMetaData)
  }

  /**
   *
   * @param data
   * @return
   */
  private def train(data : RDD[IndexerTuple]) : VectorApproximationIndexMetaData = {
    //data
    val fraction = ADAMSamplingUtils.computeFractionForSampleSize(trainingSize, data.count(), false)
    val trainData = data.sample(false, fraction)

    val dim = trainData.first.value.length

    val signatureGenerator =  new FixedSignatureGenerator(dim, bitsPerDimension)
    val marks = marksGenerator.getMarks(trainData, maxMarks)

    VectorApproximationIndexMetaData(marks, signatureGenerator, distance)
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

object VectorApproximationIndexer {
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

    val signatureGeneratorDescription = properties.getOrElse("signatureGenerator", "fixed")
    val fixedNumBitsPerDimension = properties.getOrElse("fixedNumBits", "8").toInt

    val trainingSize = properties.getOrElse("trainingSize", "5000").toInt


    new VectorApproximationIndexer(maxMarks, marksGenerator, fixedNumBitsPerDimension, trainingSize, distance)
  }
}