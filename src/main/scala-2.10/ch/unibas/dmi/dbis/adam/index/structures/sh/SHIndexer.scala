package ch.unibas.dmi.dbis.adam.index.structures.sh

import breeze.linalg.{Matrix, Vector, _}
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature.{VectorBase, _}
import ch.unibas.dmi.dbis.adam.entity.{EntityHandler, Entity}
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.index.Index.{IndexName, IndexTypeName}
import ch.unibas.dmi.dbis.adam.index._
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.main.{AdamContext, SparkStartup}
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.util.random.ADAMSamplingUtils


/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
class SHIndexer(nbits : Int, trainingSize : Int)(@transient implicit val ac : AdamContext) extends IndexGenerator with Serializable {
  @transient lazy val log = Logger.getLogger(getClass.getName)

  override val indextypename: IndexTypeName = IndexTypes.SHINDEX


  /**
   *
   * @param data
   * @return
   */
  override def index(indexname : IndexName, entityname : EntityName, data: RDD[IndexingTaskTuple]): Index = {
    val n = EntityHandler.countTuples(entityname).get
    val fraction = ADAMSamplingUtils.computeFractionForSampleSize(trainingSize, n, false)
    val trainData = data.sample(false, fraction)

    val indexMetaData = train(trainData.collect())

    log.debug("SH indexing...")

    val indexdata = data.map(
      datum => {
        val hash = SHUtils.hashFeature(datum.feature, indexMetaData)
        BitStringIndexTuple(datum.id, hash)
      })

    import SparkStartup.Implicits.sqlContext.implicits._
    new SHIndex(indexname, entityname, indexdata.toDF, indexMetaData)
  }

  /**
   *
   * @param trainData
   * @return
   */
  private def train(trainData : Array[IndexingTaskTuple]) : SHIndexMetaData = {
    log.debug("SH started training")

    val dTrainData = trainData.map(x => x.feature.map(x => x.toDouble).toArray)
    val dataMatrix = DenseMatrix(dTrainData.toList : _*)

    val nfeatures =  dTrainData.head.length

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
    val feigv = new DenseMatrix[Float](reorderEigv.rows, reorderEigv.cols, reorderEigv.toArray.map(_.toFloat))
    val projected = (dataMatrix.*(reorderEigv)).asInstanceOf[DenseMatrix[Double]]

    // fit uniform distribution
    val minProj = breeze.linalg.min(projected(::, *)).t.toDenseVector
    val maxProj = breeze.linalg.max(projected(::, *)).t.toDenseVector

    // enumerate eigenfunctions
    val maxMode = computeShareOfBits(minProj, maxProj, nbits)
    val allModes = getAllModes(maxMode, numComponents)
    val modes = getSortedModes(allModes, minProj, maxProj, nbits)

    // compute "radius" for moving query around
    val min = breeze.linalg.min(dataMatrix(*, ::)).toDenseVector
    val max = breeze.linalg.max(dataMatrix(*, ::)).toDenseVector
    val radius = 0.1 * (max - min)

    log.debug("SH finished training")

    SHIndexMetaData(feigv, minProj, maxProj, modes.toDenseMatrix, radius)
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
    val omega0 = range.mapValues(r => conv_double2vectorBase(math.Pi / math.abs(r))) //abs() added
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


object SHIndexer {
  /**
   *
   * @param properties
   */
  def apply(dimensions : Int, properties : Map[String, String] = Map[String, String]())(implicit ac : AdamContext) : IndexGenerator = {
    val nbits = math.min(500, properties.getOrElse("nbits", (dimensions * 2).toString).toInt)
    val trainingSize = properties.getOrElse("ntraining", "500").toInt

    new SHIndexer(nbits, trainingSize)
  }
}