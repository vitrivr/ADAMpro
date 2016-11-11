package org.vitrivr.adampro.index.structures.sh

import breeze.linalg.{*, DenseMatrix}
import org.vitrivr.adampro.datatypes.bitString.BitString
import org.vitrivr.adampro.datatypes.feature.Feature._

/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
private[sh] object SHUtils {
  /**
   *
   * @param f
   * @param indexMetaData
   * @return
   */
  @inline def hashFeature(f : FeatureVector, indexMetaData : SHIndexMetaData) : BitString[_] = {
    val fMat = f.toDenseVector.toDenseMatrix

    val v = fMat.*(indexMetaData.pca).asInstanceOf[DenseMatrix[Float]].toDenseVector - indexMetaData.min.toDenseVector

    val res = {
      val omegai : DenseMatrix[VectorBase] = indexMetaData.omegas(*, ::) :* v
      val ys = omegai.map(x => math.sin(x + (Math.PI / 2.0)))
      val yi = ys(*, ::).map(_.toArray.product).toDenseVector

      yi.findAll(x => x > 0)
    }

    BitString(res)
  }
}
