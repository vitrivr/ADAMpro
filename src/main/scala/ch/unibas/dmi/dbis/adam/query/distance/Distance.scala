package ch.unibas.dmi.dbis.adam.query.distance

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * August 2015
  */
object Distance {
  type Distance = Float

  implicit def conv_float2distance(value: Float): Distance = value

  implicit def conv_double2distance(value: Double): Distance = value.toFloat
}
