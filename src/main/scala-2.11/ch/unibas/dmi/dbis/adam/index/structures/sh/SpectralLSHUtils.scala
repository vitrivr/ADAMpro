package ch.unibas.dmi.dbis.adam.index.structures.sh

import breeze.linalg.{*, DenseMatrix}
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature
import Feature._
import ch.unibas.dmi.dbis.adam.datatypes.bitString.BitString
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature

/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
private[sh] object SpectralLSHUtils {
  /**
   *
   * @param f
   * @param indexMetaData
   * @return
   */
  @inline def hashFeature(f : FeatureVector, indexMetaData : SpectralLSHIndexMetaData) : BitString[_] = {
    val fMat = f.toDenseVector.toDenseMatrix
    val pca = indexMetaData.pca.toDenseMatrix

    val v = fMat.*(pca).asInstanceOf[DenseMatrix[Float]].toDenseVector - indexMetaData.min.toDenseVector

    val res = {
      val omegai : DenseMatrix[VectorBase] = indexMetaData.omegas(*, ::) :* v
      omegai :+= conv_double2vectorBase(Math.PI / 2.0)
      val ys = omegai.map(x => math.sin(x))
      val yi = ys(*, ::).map(_.toArray.product).toDenseVector

      yi.findAll(x => x > 0)
    }

    BitString(res)
  }
}
