package org.vitrivr.adampro.query

import org.vitrivr.adampro.AdamTestBase
import org.vitrivr.adampro.query.distance._
import org.scalatest.concurrent.ScalaFutures
import org.vitrivr.adampro.datatypes.vector.Vector

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2016
  *
  * Test was created using distances computed with the Julia Distances.jl code.
  */
class DistanceTestSuite extends AdamTestBase with ScalaFutures {
  val x = Vector.conv_draw2vec(Seq.fill(10)(0.toFloat))
  val y = Vector.conv_draw2vec(Seq.fill(10)(1.toFloat))
  val z = Vector.conv_draw2vec(Seq.tabulate(10)(_ * 0.1.toFloat + 0.1.toFloat))

  val w = Vector.conv_draw2vec(Seq.tabulate(10)(_ * 0.2.toFloat))

  feature("distance measures") {

    scenario("chi squared distance") {
      val f = ChiSquaredDistance

      val xydist = 10.0
      val xzdist = 5.5
      val yzdist = 2.250856127017118

      val _xydist = f(x, y)
      val _xzdist = f(x, z)
      val _yzdist = f(y, z)

      assert(math.abs(xydist - _xydist) < 10E-6)
      assert(math.abs(xzdist - _xzdist) < 10E-6)
      assert(math.abs(yzdist - _yzdist) < 10E-6)
    }

    scenario("correlation distance") {
      val f = CorrelationDistance

      val xydist = 0.0
      val xzdist = 0.0
      val yzdist = 0.0

      val _xydist = f(x, y)
      val _xzdist = f(x, z)
      val _yzdist = f(y, z)

      assert(math.abs(xydist - _xydist) < 10E-6)
      assert(math.abs(xzdist - _xzdist) < 10E-6)
      assert(math.abs(yzdist - _yzdist) < 10E-6)
    }

    scenario("cosine distance") {
      val f = CosineDistance

      val xydist = 0.0
      val xzdist = 0.0
      val yzdist = 0.11359473957208177

      val _xydist = f(x, y)
      val _xzdist = f(x, z)
      val _yzdist = f(y, z)

      assert(math.abs(xydist - _xydist) < 10E-6)
      assert(math.abs(xzdist - _xzdist) < 10E-6)
      assert(math.abs(yzdist - _yzdist) < 10E-6)
    }

    scenario("hamming distance") {
      val f = HammingDistance

      val xydist = 10.0
      val xzdist = 10.0
      val yzdist = 9.0

      //val _xydist = f(x, y)
      //val _xzdist = f(x, z)
      val _yzdist = f(y, z)

      //assert(math.abs(xydist - _xydist) < 10E-6)
      //assert(math.abs(xzdist - _xzdist) < 10E-6)
      assert(math.abs(yzdist - _yzdist) < 10E-6)
    }

    scenario("jaccard distance") {
      val f = JaccardDistance

      val xydist = 1.0
      val xzdist = 1.0
      val yzdist = 0.44999999999999996

      val _xydist = f(x, y)
      val _xzdist = f(x, z)
      val _yzdist = f(y, z)

      assert(math.abs(xydist - _xydist) < 10E-6)
      assert(math.abs(xzdist - _xzdist) < 10E-6)
      assert(math.abs(yzdist - _yzdist) < 10E-6)
    }

    scenario("kullback-leibler divergence") {
      val f = KullbackLeiblerDivergence

      val xydist = 0.0
      val xzdist = 0.0
      val yzdist = 7.921438356864942

      val _xydist = f(x, y)
      val _xzdist = f(x, z)
      val _yzdist = f(y, z)

      assert(math.abs(xydist - _xydist) < 10E-6)
      assert(math.abs(xzdist - _xzdist) < 10E-6)
      assert(math.abs(yzdist - _yzdist) < 10E-6)
    }

    scenario("chebyshev distance") {
      val f = ChebyshevDistance

      val xydist = 1.0
      val xzdist = 1.0
      val yzdist = 0.9

      val _xydist = f(x, y)
      val _xzdist = f(x, z)
      val _yzdist = f(y, z)

      assert(math.abs(xydist - _xydist) < 10E-6)
      assert(math.abs(xzdist - _xzdist) < 10E-6)
      assert(math.abs(yzdist - _yzdist) < 10E-6)
    }

    scenario("euclidean distance") {
      val f = EuclideanDistance

      val xydist = 3.1622776601683795
      val xzdist = 1.9621416870348585
      val yzdist = 1.6881943016134133

      val _xydist = f(x, y)
      val _xzdist = f(x, z)
      val _yzdist = f(y, z)

      assert(math.abs(xydist - _xydist) < 10E-6)
      assert(math.abs(xzdist - _xzdist) < 10E-6)
      assert(math.abs(yzdist - _yzdist) < 10E-6)
    }

    scenario("squared euclidean distance") {
      val f = SquaredEuclideanDistance

      val xydist = 10.0
      val xzdist = 3.85
      val yzdist = 2.8500000000000005

      val _xydist = f(x, y)
      val _xzdist = f(x, z)
      val _yzdist = f(y, z)

      assert(math.abs(xydist - _xydist) < 10E-6)
      assert(math.abs(xzdist - _xzdist) < 10E-6)
      assert(math.abs(yzdist - _yzdist) < 10E-6)
    }

    scenario("manhattan distance") {
      val f = ManhattanDistance

      val xydist = 10.0
      val xzdist = 5.5
      val yzdist = 4.5

      val _xydist = f(x, y)
      val _xzdist = f(x, z)
      val _yzdist = f(y, z)

      assert(math.abs(xydist - _xydist) < 10E-6)
      assert(math.abs(xzdist - _xzdist) < 10E-6)
      assert(math.abs(yzdist - _yzdist) < 10E-6)
    }

    scenario("minkowski distance") {
      val p = 5.5
      val f = new MinkowskiDistance(p)

      val xydist = 1.5199110829529339
      val xzdist = 1.1428468094176871
      val yzdist = 1.0148028591241314

      val _xydist = f(x, y)
      val _xzdist = f(x, z)
      val _yzdist = f(y, z)

      assert(math.abs(xydist - _xydist) < 10E-6)
      assert(math.abs(xzdist - _xzdist) < 10E-6)
      assert(math.abs(yzdist - _yzdist) < 10E-6)
    }

    scenario("span norm distance") {
      val f = SpanNormDistance

      val xydist = 0.0
      val xzdist = 0.9
      val yzdist = 0.9

      val _xydist = f(x, y)
      val _xzdist = f(x, z)
      val _yzdist = f(y, z)

      assert(math.abs(xydist - _xydist) < 10E-6)
      assert(math.abs(xzdist - _xzdist) < 10E-6)
      assert(math.abs(yzdist - _yzdist) < 10E-6)
    }

    scenario("haversine distance") {
      val f = HaversineDistance

      val xydist = 157249
      val xzdist =  24863
      val yzdist = 133893

      val _xydist = f(x, y)
      val _xzdist = f(x, z)
      val _yzdist = f(y, z)

      assert(math.abs(xydist - _xydist) < 1)
      assert(math.abs(xzdist - _xzdist) < 1)
      assert(math.abs(yzdist - _yzdist) < 1)
    }

    scenario("weighted euclidean distance") {
      val f = EuclideanDistance

      val xydist = 3.0
      val xzdist = 2.2978250586152114
      val yzdist = 1.0392304845413265

      val _xydist = f(x, y, Some(w))
      val _xzdist = f(x, z, Some(w))
      val _yzdist = f(y, z, Some(w))

      assert(math.abs(xydist - _xydist) < 10E-6)
      assert(math.abs(xzdist - _xzdist) < 10E-6)
      assert(math.abs(yzdist - _yzdist) < 10E-6)
    }

    scenario("weighted squared euclidean distance") {
      val f = SquaredEuclideanDistance

      val xydist = 9.0
      val xzdist = 5.28
      val yzdist = 1.08

      val _xydist = f(x, y, Some(w))
      val _xzdist = f(x, z, Some(w))
      val _yzdist = f(y, z, Some(w))

      assert(math.abs(xydist - _xydist) < 10E-6)
      assert(math.abs(xzdist - _xzdist) < 10E-6)
      assert(math.abs(yzdist - _yzdist) < 10E-6)
    }

    scenario("weighted manhattan distance") {
      val f = ManhattanDistance

      val xydist = 9.0
      val xzdist = 6.6
      val yzdist = 2.4

      val _xydist = f(x, y, Some(w))
      val _xzdist = f(x, z, Some(w))
      val _yzdist = f(y, z, Some(w))

      assert(math.abs(xydist - _xydist) < 10E-6)
      assert(math.abs(xzdist - _xzdist) < 10E-6)
      assert(math.abs(yzdist - _yzdist) < 10E-6)
    }

    scenario("weighted minkowski distance") {
      val p = 5.5
      val f = new MinkowskiDistance(p)

      val xydist = 1.491072079537323
      val xzdist = 1.2463626613947523
      val yzdist = 0.7298778196120014

      val _xydist = f(x, y, Some(w))
      val _xzdist = f(x, z, Some(w))
      val _yzdist = f(y, z, Some(w))

      assert(math.abs(xydist - _xydist) < 10E-6)
      assert(math.abs(xzdist - _xzdist) < 10E-6)
      assert(math.abs(yzdist - _yzdist) < 10E-6)
    }

    scenario("weighted hamming distance") {
      val f = HammingDistance

      val xydist = 9.0
      val xzdist = 9.0
      val yzdist = 7.199999999999999

      val _xydist = f(x, y, Some(w))
      val _xzdist = f(x, z, Some(w))
      val _yzdist = f(y, z, Some(w))

      assert(math.abs(xydist - _xydist) < 10E-6)
      assert(math.abs(xzdist - _xzdist) < 10E-6)
      assert(math.abs(yzdist - _yzdist) < 10E-6)
    }
  }
}
