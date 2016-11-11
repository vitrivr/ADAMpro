package org.vitrivr.adampro.index.structures.va.marks

import org.vitrivr.adampro.index.IndexingTaskTuple
import org.vitrivr.adampro.index.structures.va.VAIndex.Marks

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * September 2016
  */
private[va] object VAPlusMarksGenerator extends MarksGenerator with Serializable {
  /**
    *
    * @param samples  training samples
    * @param maxMarks maximal number of marks (different for every dimension)
    * @return
    */
  override private[va] def getMarks(samples: Array[IndexingTaskTuple[_]], maxMarks: Seq[Int]): Marks = {
    val dimensionality = maxMarks.length
    val EPSILON = 10E-9
    val init = EquidistantMarksGenerator.getMarks(samples, maxMarks).map(x => x ++ Seq((x.last + EPSILON).toFloat))

    (0 until dimensionality).map { dim =>
      var marks: Seq[Float] = init(dim)
      var delta = Float.PositiveInfinity
      var deltaBar = Float.PositiveInfinity

      //TODO: rather than K-means, use DBScan
      do {
        //K-means
        delta = deltaBar

        val points = samples.map(_.feature.apply(dim))

        val rjs = marks.sliding(2).toList.map { list => {
          val filteredPoints = points.filter(p => p >= list(0) && p < list(1))
          if (filteredPoints.isEmpty) {
            list.toArray
          } else {
            filteredPoints
          }
        }
        }.map(fps => (1.0 / fps.length) * fps.sum).map(_.toFloat)
        val cjs = Seq(rjs.head) ++ rjs.sliding(2).map(x => x.sum / x.length.toFloat).toList

        marks = cjs

        deltaBar = marks.sliding(2).map { list => points.filter(p => p >= list(0) && p < list(1)) }.toList.zip(rjs).map { case (fps, rj) => fps.map(fp => (fp - rj) * (fp - rj)).sum }.sum
      } while (deltaBar / delta < 0.999)

      marks
    }
  }
}
