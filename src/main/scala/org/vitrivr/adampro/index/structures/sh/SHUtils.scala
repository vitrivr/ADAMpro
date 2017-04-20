package org.vitrivr.adampro.index.structures.sh

import breeze.linalg.{*, DenseMatrix}
import org.vitrivr.adampro.datatypes.bitstring.BitString
import org.vitrivr.adampro.datatypes.vector.Vector._

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
  @inline def hashFeature(f : MathVector, indexMetaData : SHIndexMetaData) : BitString[_] = {
    val fMat = f.toDenseVector.toDenseMatrix

    //projected vector
    val projV = (fMat.*(indexMetaData.pca).toDenseVector - indexMetaData.min.toDenseVector).data

    val yi = indexMetaData.eigenfunctions.map{ case(dim, k, range) =>
      eigenfunction(k, range, projV(dim))
    }

    val res = yi.zipWithIndex.filter(x => x._1 > 0).map(_._2)

    BitString(res)
  }


  /**
    *
    * @param k number of eigenfunction
    * @param range range on dimension (i.e., b - a)
    * @param x value to which eigenfunction is applied
    * @return
    */
  private[sh] def eigenfunction(k : Int,  range : Double, x : Double) =  math.sin(math.Pi / 2.0 + (k * math.Pi / range) * x)


  /**
    *
    * @param k number of eigenvalue
    * @param epsilon acceptable similarity
    * @param range range on dimension (i.e., b - a)
    * @return
    */
  @deprecated("use simplifiedEigenvalue instead")
  private[sh] def eigenvalue(k : Int, epsilon : Double, range : Double) = (1 - math.pow(math.E, (- (epsilon * epsilon) / 2) * math.pow((k * math.Pi / range), 2) ))


  /**
    *
    * @param k number of eigenvalue
    * @param range range on dimension (i.e., b - a)
    * @return
    */
  private[sh] def simplifiedEigenvalue(k : Int, range : Double) = math.pow(k * math.Pi / range, 2)
}
