package ch.unibas.dmi.dbis.adam.datatypes.feature

import breeze.linalg.{DenseVector, _}
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature.FeatureVector

import scala.util.Random

/**
  * adamtwo
  *
  * Implements a feature that can be moved to some small extent given a radius.
  *
  * Ivan Giangreco
  * September 2015
  */
class MoveableFeature(val v: FeatureVector) {
  /**
    * Moves feature randomly according to the given radius (same radius for all dimensions).
    *
    * @param radius
    * @return
    */
  def move(radius: Float): FeatureVector = {
    val diff = DenseVector.fill(v.length)(radius - 2 * Random.nextFloat * radius)
    v + diff
  }

  /**
    * Moves feature randomly according to the given radius (different radius for each dimension).
    *
    * @param radius
    * @return
    */
  def move(radius: FeatureVector): FeatureVector = {
    val diff = radius - 2 * Random.nextFloat * radius
    v + diff
  }
}

object MovableFeature {
  implicit def conv_feature2MovableFeature(v: FeatureVector) = new MoveableFeature(v)
}