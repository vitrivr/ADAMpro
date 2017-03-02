package org.vitrivr.adampro.index.structures.sh

import breeze.linalg.{Matrix, sum, _}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.vitrivr.adampro.config.AttributeNames
import org.vitrivr.adampro.datatypes.vector.Vector
import org.vitrivr.adampro.datatypes.vector.Vector.{VectorBase, _}
import org.vitrivr.adampro.helpers.tracker.OperationTracker
import org.vitrivr.adampro.index.Index.IndexTypeName
import org.vitrivr.adampro.index._
import org.vitrivr.adampro.index.structures.IndexTypes
import org.vitrivr.adampro.main.AdamContext
import org.vitrivr.adampro.query.distance.DistanceFunction


/**
  * adamtwo
  *
  * Ivan Giangreco
  * August 2015
  */
class SHIndexGenerator(nbits: Option[Int], trainingSize: Int)(@transient implicit val ac: AdamContext) extends IndexGenerator {
  override val indextypename: IndexTypeName = IndexTypes.SHINDEX

  /**
    *
    * @param data raw data to index
    * @return
    */
  override def index(data: DataFrame, attribute : String)(tracker : OperationTracker): (DataFrame, Serializable) = {
    log.trace("SH started indexing")

    val meta = train(getSample(math.max(trainingSize, MINIMUM_NUMBER_OF_TUPLE), attribute)(data))

    val cellUDF = udf((c: DenseSparkVector) => {
      SHUtils.hashFeature(Vector.conv_dspark2vec(c), meta).serialize
    })

    val indexed = data.withColumn(AttributeNames.featureIndexColumnName, cellUDF(data(attribute)))

    log.trace("SH finished indexing")

    (indexed, meta)
  }

  /**
    *
    * @param trainData
    * @return
    */
  private def train(trainData: Seq[IndexingTaskTuple]): SHIndexMetaData = {
    log.trace("SH started training")

    val dTrainData = trainData.map(x => x.ap_indexable.map(x => x.toDouble).toArray)
    val dataMatrix = DenseMatrix(dTrainData.toList: _*)

    val nfeatures = dTrainData.head.length

    //TODO: error in creation because dimensionality is wrong?

    val numComponents = math.min(nfeatures, nbits.getOrElse(nfeatures * 2))

    // pca
    val covs = cov(dataMatrix, true)
    val eig = eigSym(covs)
    val cols = ((eig.eigenvalues.length - numComponents) until (eig.eigenvalues.length))
    val eigv = eig.eigenvectors(::, cols)
    val reorderPerm = DenseMatrix.tabulate(numComponents, numComponents) { case (i, j) => {
      if (i == numComponents - 1 - j) {
        1.toDouble
      }
      else {
        0.toDouble
      }
    }
    }
    val reorderEigv = eigv * reorderPerm
    val feigv = new DenseMatrix[VectorBase](reorderEigv.rows, reorderEigv.cols, reorderEigv.toArray.map(Vector.conv_double2vb))
    val projected = (dataMatrix.*(reorderEigv)).asInstanceOf[DenseMatrix[Double]]

    // fit uniform distribution
    val minProj = breeze.linalg.min(projected(::, *)).t.toDenseVector
    val maxProj = breeze.linalg.max(projected(::, *)).t.toDenseVector

    // enumerate eigenfunctions
    val maxMode = computeShareOfBits(minProj, maxProj, nbits.getOrElse(nfeatures * 2))
    val allModes = getAllModes(maxMode, numComponents)
    val modes = getSortedModes(allModes, minProj, maxProj, nbits.getOrElse(nfeatures * 2))

    // compute "radius" for moving query around
    val radius = 0.1 * (maxProj - minProj)

    log.trace("SH finished training")

    SHIndexMetaData(feigv, minProj, maxProj, modes.toDenseMatrix, radius)
  }


  /**
    *
    * @param min
    * @param max
    * @param bits
    * @return
    */
  private def computeShareOfBits(min: Vector[VectorBase], max: Vector[VectorBase], bits: Int): Array[Int] = {
    val range = max - min
    (range * ((bits + 1) / breeze.linalg.max(range))).map(x => math.ceil(x).toInt - 1).toArray
  }

  /**
    *
    * @param maxMode
    * @param numComponents
    * @return
    */
  private def getAllModes(maxMode: Array[Int], numComponents: Int): DenseMatrix[VectorBase] = {
    val modesNum = sum(maxMode) + 1
    val modes: DenseMatrix[VectorBase] = DenseMatrix.zeros[VectorBase](modesNum, numComponents)

    var pos = 0
    (0 until numComponents).foreach { nc =>
      (1 to maxMode(nc)).foreach { m =>
        modes(pos + m, nc) = m
      }
      pos += maxMode(nc)
    }

    modes
  }

  /**
    *
    * @param modes
    * @param min
    * @param max
    * @param bits
    * @return
    */
  private def getSortedModes(modes: DenseMatrix[VectorBase], min: Vector[VectorBase], max: Vector[VectorBase], bits: Int): Matrix[VectorBase] = {
    val range = max - min
    val omega0 = range.mapValues(r => Vector.conv_double2vb(math.Pi / math.abs(r))) //abs() added
    val omegas = modes(*, ::).:*(omega0)
    val omegas2 = omegas :* omegas
    val eigVal = sum(omegas2(*, ::))

    val sortOrder = eigVal.toArray.zipWithIndex.sortBy(x => x._1).map(x => x._2) //removed reverse

    val selectedModes: DenseMatrix[VectorBase] = DenseMatrix.zeros[VectorBase](bits, modes.cols)
    sortOrder.drop(1).take(bits).zipWithIndex.foreach {
      case (so, idx) =>
        selectedModes(idx, ::).:=(modes(so, ::))
    }
    selectedModes
  }
}


class SHIndexGeneratorFactory extends IndexGeneratorFactory {
  /**
    * @param distance   distance function
    * @param properties indexing properties
    */
  def getIndexGenerator(distance: DistanceFunction, properties: Map[String, String] = Map[String, String]())(implicit ac: AdamContext): IndexGenerator = {
    val nbits = if (properties.get("nbits").isDefined) {
      Some(properties.get("nbits").get.toInt)
    } else {
      None
    }
    val trainingSize = properties.getOrElse("ntraining", "1000").toInt

    new SHIndexGenerator(nbits, trainingSize)
  }

  /**
    *
    * @return
    */
  override def parametersInfo: Seq[ParameterInfo] = Seq(
    new ParameterInfo("ntraining", "number of training tuples", Seq[String]()),
    new ParameterInfo("nbits", "number of bits for hash", Seq(64, 128, 256, 512, 1024).map(_.toString))
  )
}