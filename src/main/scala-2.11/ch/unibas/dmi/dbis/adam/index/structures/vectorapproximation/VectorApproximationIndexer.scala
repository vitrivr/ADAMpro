package ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation

import ch.unibas.dmi.dbis.adam.data.Tuple.TupleID
import ch.unibas.dmi.dbis.adam.data.types.Feature.{VectorBase, WorkingVector}
import ch.unibas.dmi.dbis.adam.data.{IndexMetaBuilder, IndexTuple}
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.VectorApproximationIndexer.{Bounds, Marks, Signature}
import ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.marks.{EquidistantMarksGenerator, EquifrequentMarksGenerator, MarksGenerator}
import ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.results.{BoundableResultHandler, ResultHandler}
import ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.signature.{FixedSignatureGenerator, SignatureGenerator}
import ch.unibas.dmi.dbis.adam.index.{Index, IndexGenerator, IndexScanner}
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.query.distance.Distance._
import ch.unibas.dmi.dbis.adam.query.distance.NormBasedDistanceFunction
import ch.unibas.dmi.dbis.adam.table.Table._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.{Map => mMap}


/**
 * 
 */
class VectorApproximationIndexer(maxMarks: Int = 64, sampleSize: Int = 500, marksGenerator: MarksGenerator, signatureGenerator: SignatureGenerator) extends IndexGenerator with IndexScanner with Serializable {
  override val indexname: String = "va"

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
  override def index(indexname : IndexName, tablename : TableName, data: RDD[IndexTuple[WorkingVector]]): Index = {
    val marks = computeMarks(data)

    val indexdata = data.map(
      datum => {
        val cells = getCells(datum.value, marks)
        val signature = signatureGenerator.toSignature(cells)
        IndexTuple(datum.tid, signature)
      })

    val metadataBuilder = new IndexMetaBuilder()
    metadataBuilder.put("marks", marks)
    metadataBuilder.put("signatureGenerator", signatureGenerator)

    import SparkStartup.sqlContext.implicits._
    Index(indexname, tablename, indexdata.toDF, metadataBuilder.build())
  }

  /**
   * 
   */
  def query(q: WorkingVector, index: Index, options: Map[String, Any]): Seq[TupleID] = {
    val data = index.index
    val metadata = index.indexMeta

    val marks = metadata.get("marks").asInstanceOf[Marks]
    val signatureGenerator = metadata.get("signatureGenerator").asInstanceOf[SignatureGenerator]

    val k = options("k").asInstanceOf[Integer]
    val norm = options("norm").asInstanceOf[Integer]

    val lbounds: Bounds = lowerBounds(q, marks, new NormBasedDistanceFunction(norm))
    val ubounds: Bounds = upperBounds(q, marks, new NormBasedDistanceFunction(norm))

    val localResultHandlers = data
      .map{ tuple => IndexTuple(tuple.getLong(0), tuple.getAs[Signature](1)) }
      .mapPartitions(indexTuplesIt => {
      val rh = new BoundableResultHandler(k, lbounds, ubounds, signatureGenerator)
      rh.offerIndexTuple(indexTuplesIt)
      rh.iterator
    })

    val globalResultHandler = new ResultHandler(k)
    globalResultHandler.offerResultElement(localResultHandlers.collect.toSeq.iterator)

    globalResultHandler.results.map(_.indexTuple.tid)
  }

  /**
   * 
   */
  def lowerBounds(q: WorkingVector, marks: => Marks, distance: NormBasedDistanceFunction): Bounds = {
    val bounds = marks.zipWithIndex.map {
      case (dimMarks, i) =>
        val fvi = q(i)
        dimMarks.iterator.sliding(2).withPartial(false).map { //TODO: valuesIterator?
          dimMark =>
            if (fvi < dimMark(0)) {
              distance(dimMark(0), fvi)
            } else if (fvi > dimMark(1)) {
              distance(fvi, dimMark(1))
            } else {
              0.toFloat
            }
        }.toArray
    }.toArray

    bounds
  }

  /**
   * 
   */
  def upperBounds(q: WorkingVector, marks: => Marks, distance: NormBasedDistanceFunction): Bounds = {
    val bounds = marks.zipWithIndex.map {
      case (dimMarks, i) =>
        val fvi = q(i)

        dimMarks.iterator.sliding(2).withPartial(false).map { //TODO: valuesIterator?
          dimMark =>
            if (fvi <= (dimMark(0) + dimMark(1)) / 2.toFloat) {
              distance(dimMark(1), fvi)
            } else {
              distance(fvi, dimMark(0))
            }
        }.toArray
    }.toArray

    bounds
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