package org.vitrivr.adampro.query.planner

import org.vitrivr.adampro.utils.exception.GeneralAdamException
import org.vitrivr.adampro.process.SharedComponentContext
import org.vitrivr.adampro.utils.Logging

import scala.collection.mutable

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * December 2016
  */
class PlannerRegistry() extends Logging {
  val handlers = mutable.Map[String, PlannerHeuristics]()

  /**
    *
    * @param name
    * @return
    */
  def apply(name: String)(implicit ac : SharedComponentContext): Option[QueryPlanner] = {
    val res = handlers.get(name)

    if (res.isEmpty) {
      throw new GeneralAdamException("no suitable optimizer heuristic found in registry named " + name)
    }

    res.map(new QueryPlanner(_)(ac))
  }

  /**
    *
    * @param name
    * @param optimizer
    */
  def register(name: String, optimizer : PlannerHeuristics): Unit = {
    try {
      handlers += name -> optimizer
    } catch {
      case e: Exception => log.error("error in registering optimizer for " + name, e)
    }
  }
}

object PlannerRegistry {
  val registry = new PlannerRegistry()
  registry.register("naive", new NaiveHeuristics())
  registry.register("svm", new SVMPlannerHeuristics())
  registry.register("lr", new LRPlannerHeuristics())

  /**
    *
    * @param name
    * @return
    */
  def apply(name : String)(implicit ac : SharedComponentContext) = registry(name)
}