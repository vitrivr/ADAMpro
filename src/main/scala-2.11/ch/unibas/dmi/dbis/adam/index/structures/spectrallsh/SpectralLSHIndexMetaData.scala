package ch.unibas.dmi.dbis.adam.index.structures.spectrallsh

import breeze.linalg._
import ch.unibas.dmi.dbis.adam.data.types.Feature._

/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
private[spectrallsh] case class SpectralLSHIndexMetaData(pca : DenseMatrix[VectorBase], min : DenseVector[VectorBase], max : DenseVector[VectorBase], modes : DenseMatrix[VectorBase], radius : DenseVector[VectorBase]) {
  lazy val omegas: DenseMatrix[VectorBase] = {
    val range = max - min
    val omega0 = range.mapValues(r => conv_double2vectorBase(math.Pi / r))
    val modesMat = modes.toDenseMatrix
    val omegas : DenseMatrix[VectorBase] = (modesMat(*, ::).:*(omega0)).toDenseMatrix

    omegas
  }
}
