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
private[sh] case class SHIndexMetaData (pca : DenseMatrix[VectorBase], min : MathVector, max : MathVector, eigenfunctions : Array[(Int, Int, VectorBase)], radius : MathVector)  extends Serializable {
}
