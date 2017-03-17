package org.vitrivr.adampro.query.distance

import org.vitrivr.adampro.datatypes.vector.Vector._
import org.vitrivr.adampro.query.distance.Distance._
import org.vitrivr.adampro.utils.Logging
import java.lang.Math

/**
  * ADAMpro
  *
  * Lukas Beck
  * March 2017
  *
  * The haversine distance is a formula to calculate the great-circle distance on earth, given two points. The distance
  * is calculated on the basis of a spherical earth and according to the source gives errors typically up to 0.3%.
  * The distance function requires at least two-dimensional input vectors, expects latitude in the first and
  * longitude in the second dimension, both in degrees and returns the distance in meters. Values beyond the second
  * dimension and weights in general are ignored.
  * From: http://www.movable-type.co.uk/scripts/latlong.html
  */
object HaversineDistance extends DistanceFunction with Logging with Serializable {
  val RADIUS_EARTH = 6371000

  override def apply(v1: MathVector, v2: MathVector, weights: Option[MathVector]): Distance = {
    if (weights.isDefined) {
      log.warn("weights cannot be used with haversine distance and are ignored")
    }

    if (v1.length > 2 || v2.length > 2) {
      log.warn(s"haversine distance ignores values beyond the first and second dimension, found ${Math.max(v1.length, v2.length)} dimensions")
    }

    if (v1.length < 2 || v2.length < 2) {
      log.error(s"haversine distance requires two-dimensional input, but found ${Math.min(v1.length, v2.length)} dimensions, input: $v1 and $v2")
      Double.PositiveInfinity
    } else {
      val lat1 = normalizeLat(v1(0))
      val lng1 = normalizeLng(v1(1))
      val lat2 = normalizeLat(v2(0))
      val lng2 = normalizeLng(v2(1))

      val φ1 = degreeToRadian(lat1)
      val φ2 = degreeToRadian(lat2)
      val λ1 = degreeToRadian(lng1)
      val λ2 = degreeToRadian(lng2)
      val Δφ = φ2 - φ1
      val Δλ = λ2 - λ1

      val a = Math.sin(Δφ / 2.0) * Math.sin(Δφ / 2.0) + Math.cos(φ1) * Math.cos(φ2) * Math.sin(Δλ / 2.0) * Math.sin(Δλ / 2.0)
      2 * RADIUS_EARTH * Math.atan2(Math.sqrt(a), Math.sqrt(1.0 - a))
    }
  }

  def floorMod(dividend: Double, divisor: Double): Double = (dividend % divisor + divisor) % divisor
  def normalizeLat(lat: Double): Double = {
    val latMod = floorMod(lat, 180)
    if (latMod < 90) { latMod } else { latMod - 180 }
  }
  def normalizeLng(lng: Double): Double = {
    val lngMod = floorMod(lng, 360)
    if (lngMod < 180) { lngMod } else { lngMod - 360 }
  }
  def degreeToRadian(degrees: Double): Double = degrees * Math.PI / 180
}
