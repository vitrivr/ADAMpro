package org.vitrivr.adampro.datatypes.feature

import breeze.linalg.{DenseVector, _}
import org.vitrivr.adampro.datatypes.feature.Feature.FeatureVector

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
    * @param radius radius which is the maximum used for moving feature around
    * @return
    */
  def move(radius: Float): FeatureVector = {
    val diff = DenseVector.fill(v.length)(radius - 2 * Random.nextFloat * radius)
    v + diff
  }

  /**
    * Moves feature randomly according to the given radius (different radius for each dimension).
    *
    * @param radius feature vector which contains a radius in each dimension which is the maximum used for moving feature around
    * @return
    */
  def move(radius: FeatureVector): FeatureVector = {
    val diff = radius - 2 * Random.nextFloat * radius
    v + diff
  }
}

object MovableFeature {
  implicit def conv_feature2MovableFeature(v: FeatureVector): MoveableFeature = new MoveableFeature(v)
}