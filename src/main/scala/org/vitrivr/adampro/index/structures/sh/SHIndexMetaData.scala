package org.vitrivr.adampro.index.structures.sh

import breeze.linalg._
import org.vitrivr.adampro.datatypes.feature.Feature
import org.vitrivr.adampro.datatypes.feature.Feature._

/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
@SerialVersionUID(100L)
private[sh] case class SHIndexMetaData (pca : DenseMatrix[VectorBase], min : FeatureVector, max : FeatureVector, modes : DenseMatrix[VectorBase], radius : FeatureVector)  extends Serializable {
  lazy val omegas: DenseMatrix[VectorBase] = {
    val range = max - min
    val omega0 = range.mapValues(r => conv_double2vectorBase(math.Pi / r))
    val modesMat = modes.toDenseMatrix
    val omegas : DenseMatrix[VectorBase] = (modesMat(*, ::).:*(omega0)).toDenseMatrix

    omegas
  }
}