package org.vitrivr.adampro.query.optimizer

import org.vitrivr.adampro.exception.GeneralAdamException
import org.vitrivr.adampro.main.AdamContext
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
  def apply(name: String): Option[OptimizerOp] = {
    val res = handlers.get(name)

    if (res.isEmpty) {
      throw new GeneralAdamException("no suitable optimizer heuristic found in registry named " + name)
    }

    res.map(new OptimizerOp(_))
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
  def loadDefault()(implicit ac: AdamContext): Unit ={
    ac.optimizerRegistry.value.register("scored", new NaiveOptimizerHeuristic())
    ac.optimizerRegistry.value.register("empirical", new SVMOptimizerHeuristic())
  }
}