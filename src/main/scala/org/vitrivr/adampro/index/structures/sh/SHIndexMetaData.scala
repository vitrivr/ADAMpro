package org.vitrivr.adampro.index.structures.sh

import breeze.linalg._
import org.vitrivr.adampro.datatypes.vector.Vector._
import org.vitrivr.adampro.datatypes.vector.Vector

/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
@SerialVersionUID(100L)
private[sh] case class SHIndexMetaData (pca : DenseMatrix[VectorBase], min : MathVector, max : MathVector, modes : DenseMatrix[VectorBase], radius : MathVector)  extends Serializable {
  lazy val omegas: DenseMatrix[VectorBase] = {
    val range = max - min
    val omega0 = range.mapValues(r => Vector.conv_double2vb(math.Pi / r))
    val modesMat = modes.toDenseMatrix
    val omegas : DenseMatrix[VectorBase] = (modesMat(*, ::).:*(omega0)).toDenseMatrix

    omegas
  }
}