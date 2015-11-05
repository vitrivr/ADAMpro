package ch.unibas.dmi.dbis.adam.datatypes.feature

import breeze.linalg.{DenseVector, _}
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature.FeatureVector

import scala.util.Random

/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
class MoveableFeature(val v: FeatureVector) {
  def move(radius : Double) : FeatureVector = move(radius.toFloat)

  def move(radius : Float) : FeatureVector = {
      val diff = DenseVector.fill(v.length)(radius - 2 * Random.nextFloat * radius)
      v + diff
  }

  def move(radius : FeatureVector) : FeatureVector = {
    val diff = radius - 2 * Random.nextFloat * radius
    v + diff
  }
}

object MovableFeature {
  implicit def conv_feature2MovableFeature(v: FeatureVector) = new MoveableFeature(v)
}