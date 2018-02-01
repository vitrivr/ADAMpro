package org.vitrivr.adampro.query.planner

import org.scalatest.concurrent.ScalaFutures
import org.vitrivr.adampro.AdamTestBase
import org.vitrivr.adampro.communication.api.IndexOp
import org.vitrivr.adampro.data.entity.Entity
import org.vitrivr.adampro.data.index.Index
import org.vitrivr.adampro.data.index.structures.IndexTypes
import org.vitrivr.adampro.query.distance.EuclideanDistance
import org.vitrivr.adampro.data.datatypes.vector.{ADAMNumericalVector, Vector}
import org.vitrivr.adampro.query.query.RankingQuery

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

        val optimizer = new SVMPlannerHeuristics(3)
        val optimizerOp = new QueryPlanner(optimizer)

        optimizerOp.train(entity.get, ManualIndexCollection(lb)(ac), RandomQueryCollection(entityname, "feature", 2)(ac), Map())

        val vec = Seq.fill(ndims)(Vector.nextRandom())
        val nnq = RankingQuery("feature", ADAMNumericalVector(Vector.conv_draw2vec(vec)), None, EuclideanDistance, 100, true)

        val res = lb.map(index => index.indexname -> optimizerOp.getScore(index, nnq)).toMap

        log.info("adjusted weights: " + res.mkString("; "))

        res.foreach(x => assert(x._2 > 0))
      }
    }
  }
}
