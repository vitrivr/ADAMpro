package org.vitrivr.adampro.index.structures.sh

import breeze.linalg._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.vitrivr.adampro.config.AttributeNames
import org.vitrivr.adampro.datatypes.vector.Vector
import org.vitrivr.adampro.datatypes.vector.Vector.{VectorBase, _}
import org.vitrivr.adampro.exception.GeneralAdamException
import org.vitrivr.adampro.helpers.tracker.OperationTracker
import org.vitrivr.adampro.index.Index.IndexTypeName
import org.vitrivr.adampro.index._
import org.vitrivr.adampro.index.structures.IndexTypes
import org.vitrivr.adampro.main.AdamContext
import org.vitrivr.adampro.query.distance.{DistanceFunction, EuclideanDistance}


/**
  * adamtwo
  *
  * Ivan Giangreco
  * August 2015
  */
class SHIndexGenerator(nbits_total: Option[Int], trainingSize: Int)(@transient implicit val ac: AdamContext) extends IndexGenerator {
  override val indextypename: IndexTypeName = IndexTypes.SHINDEX

  /**
    *
    * @param data raw data to index
    * @return
    */
  override def index(data: DataFrame, attribute: String)(tracker: OperationTracker): (DataFrame, Serializable) = {
    log.trace("SH started indexing")

    val meta = train(getSample(math.max(trainingSize, MINIMUM_NUMBER_OF_TUPLE), attribute)(data))
    val metaBc = ac.sc.broadcast(meta)
    tracker.addBroadcast(metaBc)

    val cellUDF = udf((c: DenseSparkVector) => {
      SHUtils.hashFeature(Vector.conv_dspark2vec(c), metaBc.value).serialize
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

    val doubleTrainData = trainData.map(x => x.ap_indexable.map(x => x.toDouble).toArray)
    val dataMatrix = DenseMatrix(doubleTrainData.toList: _*)

    val ndims = doubleTrainData.head.length

    //TODO: error in creation because dimensionality is wrong?

    val numComponents = math.min(ndims, nbits_total.getOrElse(ndims * 2))

    // pca
    val covs = cov(dataMatrix, center = true)
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
    val projected = (dataMatrix.*(reorderEigv))

    // number of bits to use
    val nbits = nbits_total.getOrElse(ndims * 2)

    // fit uniform distribution
    val minProj = breeze.linalg.min(projected(::, *)).t.toDenseVector.map(Vector.conv_double2vb)
    val maxProj = breeze.linalg.max(projected(::, *)).t.toDenseVector.map(Vector.conv_double2vb)

    // enumerate eigenfunctions
    val ranges = (maxProj - minProj).toArray
    val maxRange = ranges.max

    val eigenfuncs = ranges.zipWithIndex.flatMap { case (range, ndim) =>
      val nmodes = shareOfBits(nbits, range, maxRange) //number of modes for dimension
      (1 to nmodes).map(k => (SHUtils.simplifiedEigenvalue(k, range), ndim, k, range)) //enumerate eigenfunctions
    } .sortBy(_._1) //sort by eigenvalues
      .take(nbits) //take only limited number of eigenfunctions
      .map(x => (x._2, x._3, x._4)) //dim, k, range
      .toArray

    // compute "radius" for moving query around
    val radius = Vector.conv_double2vb(0.1) * (maxProj - minProj)

    log.trace("SH finished training")

    SHIndexMetaData(feigv, minProj, maxProj, eigenfuncs, radius)
  }



    /**
    *
    * @param bits number of bits
    * @param range range on dimension (i.e., b - a)
    * @param max max value on ranges
    * @return
    */
  private def shareOfBits(bits : Int, range : Double, max : Double) = math.ceil(range * ((bits + 1) / max)).toInt - 1
}


class SHIndexGeneratorFactory extends IndexGeneratorFactory {
  /**
    * @param distance   distance function
    * @param properties indexing properties
    */
  def getIndexGenerator(distance: DistanceFunction, properties: Map[String, String] = Map[String, String]())(implicit ac: AdamContext): IndexGenerator = {
    if (distance != EuclideanDistance) {
      throw new GeneralAdamException("SH index only supports Euclidean distance")
    }

    assert(distance == EuclideanDistance)

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