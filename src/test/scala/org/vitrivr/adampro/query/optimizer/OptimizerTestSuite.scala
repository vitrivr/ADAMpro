package org.vitrivr.adampro.query.optimizer

import org.scalatest.concurrent.ScalaFutures
import org.vitrivr.adampro.AdamTestBase
import org.vitrivr.adampro.api.IndexOp
import org.vitrivr.adampro.entity.Entity
import org.vitrivr.adampro.index.Index
import org.vitrivr.adampro.index.structures.IndexTypes
import org.vitrivr.adampro.query.distance.EuclideanDistance
import org.vitrivr.adampro.datatypes.vector.Vector
import org.vitrivr.adampro.query.query.NearestNeighbourQuery

import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * February 2017
  */
class OptimizerTestSuite extends AdamTestBase with ScalaFutures {

  val ntuples = 1000 + Random.nextInt(1000)
  val ndims = 100

  feature("svm optimizer") {

    scenario("train optimizer for indexes") {
      withSimpleEntity(ntuples, ndims) { entityname =>
        val entity = Entity.load(entityname)
        assert(entity.isSuccess)

        val lb = new ListBuffer[Index]()

        lb += IndexOp.create(entityname, "feature", IndexTypes.VAVINDEX, EuclideanDistance)().get
        lb += IndexOp.create(entityname, "feature", IndexTypes.VAFINDEX, EuclideanDistance)().get
        lb += IndexOp.create(entityname, "feature", IndexTypes.VAPLUSINDEX, EuclideanDistance)().get

        val optimizer = new SVMOptimizerHeuristic(3)
        val optimizerOp = new QueryOptimizer(optimizer)

        optimizerOp.train(ManualIndexCollection(lb)(ac), RandomQueryCollection(entityname, "feature", 2)(ac))

        val vec = Seq.fill(ndims)(Vector.nextRandom())
        val nnq = NearestNeighbourQuery("feature", Vector.conv_draw2vec(vec), None, EuclideanDistance, 100, true)

        val res = lb.map(index => index.indexname -> optimizerOp.getScore(index, nnq)).toMap

        log.info("adjusted weights: " + res.mkString("; "))

        res.foreach(x => assert(x._2 > 0))
      }
    }
  }
}
