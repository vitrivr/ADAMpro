package org.vitrivr.adampro.query.optimizer

import org.vitrivr.adampro.exception.GeneralAdamException
import org.vitrivr.adampro.main.SharedComponentContext
import org.vitrivr.adampro.utils.Logging

import scala.collection.mutable

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * December 2016
  */
class OptimizerRegistry() extends Logging {
  val handlers = mutable.Map[String, OptimizerHeuristic]()

  /**
    *
    * @param name
    * @return
    */
  def apply(name: String): Option[QueryOptimizer] = {
    val res = handlers.get(name)

    if (res.isEmpty) {
      throw new GeneralAdamException("no suitable optimizer heuristic found in registry named " + name)
    }

    res.map(new QueryOptimizer(_))
  }

  /**
    *
    * @param name
    * @param optimizer
    */
  def register(name: String, optimizer : OptimizerHeuristic): Unit = {
    try {
      handlers += name -> optimizer
    } catch {
      case e: Exception => log.error("error in registering optimizer for " + name, e)
    }
  }
}

object OptimizerRegistry {
  def loadDefault()(implicit ac: SharedComponentContext): Unit ={
    ac.optimizerRegistry.value.register("naive", new NaiveOptimizerHeuristic())
    ac.optimizerRegistry.value.register("svm", new SVMOptimizerHeuristic())
  }
}