package ch.unibas.dmi.dbis.adam.index.structures.spectrallsh

import breeze.linalg._
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature
import Feature._

/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
private[spectrallsh]
case class SpectralLSHIndexMetaData
  (pca : DenseMatrix[VectorBase], min : FeatureVector, max : FeatureVector, modes : DenseMatrix[VectorBase], radius : FeatureVector) {
  lazy val omegas: DenseMatrix[VectorBase] = {
    val range = max - min
    val omega0 = range.mapValues(r => conv_double2vectorBase(math.Pi / r))
    val modesMat = modes.toDenseMatrix
    val omegas : DenseMatrix[VectorBase] = (modesMat(*, ::).:*(omega0)).toDenseMatrix

    omegas
  }
}