package ch.unibas.dmi.dbis.adam.index.structures.spectrallsh

import breeze.linalg.{Matrix, Vector, _}
import ch.unibas.dmi.dbis.adam.data.IndexTuple
import ch.unibas.dmi.dbis.adam.data.types.Feature.{VectorBase, _}
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.index.{Index, IndexGenerator}
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.table.Table._
import org.apache.spark.rdd.RDD



/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
class SpectralLSHIndexer(nfeatures : Int, nbits : Int, trainingSize : Int) extends IndexGenerator with Serializable {
  override val indextypename : String = "slsh"


  /**
   *
   * @param data
   * @return
   */
  override def index(indexname : IndexName, tablename : TableName, data: RDD[IndexTuple[WorkingVector]]): Index = {
    val trainResult = train(data)

    val indexdata = data.map(
      datum => {
        val hash = SpectralLSHUtils.hashFeature(datum.value, trainResult)
        IndexTuple(datum.tid, hash.toByteArray)
      })


    import SparkStartup.sqlContext.implicits._
    new SpectralLSHIndex(indexname, tablename, indexdata.toDF, trainResult)
  }

  /**
   *
   * @param data
   * @return
   */
  private def train(data : RDD[IndexTuple[WorkingVector]]) : TrainResult = {
    //data
    val trainData = data.map(x => x.value.map(x => x.toDouble).toArray)
    val dataMatrix = DenseMatrix(trainData.take(trainingSize).toList : _*)

    val numComponents = math.min(nfeatures, nbits)

    // pca
    val covs = cov(dataMatrix, true)
    val eig = eigSym(covs)
    val cols = ((eig.eigenvalues.length - numComponents) until (eig.eigenvalues.length))
    val eigv = eig.eigenvectors(::, cols)
    val reorderPerm = DenseMatrix.tabulate(numComponents, numComponents){case (i, j) => {
      if(i == numComponents - 1 - j) { 1.toDouble }
      else { 0.toDouble }
    }}
    val reorderEigv = eigv * reorderPerm
    val feigv = new DenseMatrix[Float](reorderEigv.rows, reorderEigv.cols, reorderEigv.toArray.map(x => x.toFloat))
    val projected = (dataMatrix.*(reorderEigv)).asInstanceOf[DenseMatrix[Double]]

    // fit uniform distribution
    val min = breeze.linalg.min(projected(::, *)).toDenseVector
    val max = breeze.linalg.max(projected(::, *)).toDenseVector

    // enumerate eigenfunctions
    val maxMode = computeShareOfBits(min, max, nbits)
    val allModes = getAllModes(maxMode, numComponents)
    val modes = getSortedModes(allModes, min, max, nbits)

    TrainResult(feigv, min, max, modes)
  }


  /**
   *
   * @param min
   * @param max
   * @param nbits
   * @return
   */
  private def computeShareOfBits(min : Vector[VectorBase], max : Vector[VectorBase], nbits : Int) : Array[Int] = {
    val range = max - min
    (range * ((nbits + 1) / breeze.linalg.max(range))).map(x => math.ceil(x).toInt - 1).toArray
  }

  /**
   *
   * @param maxMode
   * @param numComponents
   * @return
   */
  private def getAllModes(maxMode : Array[Int], numComponents : Int) : DenseMatrix[VectorBase] = {
    val modesNum = sum(maxMode) + 1
    val modes : DenseMatrix[VectorBase] = DenseMatrix.zeros[VectorBase](modesNum, numComponents)

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
   * @param nbits
   * @return
   */
  private def getSortedModes(modes : DenseMatrix[VectorBase], min : Vector[VectorBase], max : Vector[VectorBase], nbits : Int) : Matrix[VectorBase] = {
    val range = max - min
    val omega0 = range.mapValues(r => toVectorBase(math.Pi / math.abs(r))) //abs() added
    val omegas = modes(*, ::).:*(omega0)
    val omegas2 = omegas :* omegas
    val eigVal = sum(omegas2(*, ::))

    val sortOrder = eigVal.toArray.zipWithIndex.sortBy(x => x._1).map(x => x._2) //removed reverse

    val selectedModes : DenseMatrix[VectorBase] = DenseMatrix.zeros[VectorBase](nbits, modes.cols)
    sortOrder.drop(1).take(nbits).zipWithIndex.foreach {
      case (so, idx) =>
        selectedModes(idx, ::).:=(modes(so, ::))
    }
    selectedModes
  }
}


object SpectralLSHIndexer {

  /**
   *
   * @param properties
   */
  def apply(properties : Map[String, String] = Map[String, String](), data: RDD[IndexTuple[WorkingVector]]) : IndexGenerator = {
    val nfeatures = properties.getOrElse("nfeatures", "100").toInt //TODO get from data
    val nbits = properties.getOrElse("nbits", "100").toInt
    val trainingSize = properties.getOrElse("trainingSize", "200000").toInt

    new SpectralLSHIndexer(nfeatures, nbits,trainingSize)
  }
}

/**
 *
 * @param pca
 * @param min
 * @param max
 * @param modes
 */
private case class TrainResult(pca : Matrix[VectorBase], min : Vector[VectorBase], max : Vector[VectorBase], modes : Matrix[VectorBase]) {
  lazy val omegas: DenseMatrix[VectorBase] = {
    val range = max - min
    val omega0 = range.mapValues(r => (math.Pi / r).toFloat)
    val modesMat = modes.toDenseMatrix
    val omegas : DenseMatrix[VectorBase] = (modesMat(*, ::).:*(omega0)).toDenseMatrix

    omegas
  }
}