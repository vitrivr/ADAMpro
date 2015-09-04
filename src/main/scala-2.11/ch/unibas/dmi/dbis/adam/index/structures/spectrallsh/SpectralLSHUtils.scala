package ch.unibas.dmi.dbis.adam.index.structures.spectrallsh

import breeze.linalg.{*, DenseMatrix}
import ch.unibas.dmi.dbis.adam.data.types.Feature._
import ch.unibas.dmi.dbis.adam.data.types.bitString.BitString
import ch.unibas.dmi.dbis.adam.data.types.bitString.BitString._

/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
private[spectrallsh] object SpectralLSHUtils {
  /**
   *
   * @param f
   * @param trainResult
   * @return
   */
  @inline def hashFeature(f : WorkingVector, trainResult : TrainResult) : BitStringType = {
    val fMat = f.toDenseMatrix
    val pca = trainResult.pca.toDenseMatrix

    val v = fMat.*(pca).asInstanceOf[DenseMatrix[Float]].toDenseVector - trainResult.min.toDenseVector

    val res = {
      val omegai : DenseMatrix[VectorBase] = trainResult.omegas(*, ::) :* v
      omegai :+= toVectorBase(Math.PI / 2.0)
      val ys = omegai.map(x => math.sin(x))
      val yi = ys(*, ::).map(_.toArray.product).toDenseVector

      yi.findAll(x => x > 0)
    }

    BitString.fromBitIndicesToSet(res)
  }
}
