package org.apache.spark.util.random

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
object Sampling {
  def computeFractionForSampleSize(sampleSizeLowerBound: Int, total: Long, withReplacement: Boolean): Double =
    SamplingUtils.computeFractionForSampleSize(sampleSizeLowerBound, total, withReplacement)
}
