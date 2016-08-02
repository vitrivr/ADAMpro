package ch.unibas.dmi.dbis.adam.index.structures.sh

import breeze.linalg.{*, DenseMatrix}
import ch.unibas.dmi.dbis.adam.datatypes.bitString.BitString
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._

/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
object SHUtils {
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
