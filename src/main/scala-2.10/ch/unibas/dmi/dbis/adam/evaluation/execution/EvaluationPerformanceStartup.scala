package ch.unibas.dmi.dbis.adam.evaluation.execution

import ch.unibas.dmi.dbis.adam.config.AdamConfig
import ch.unibas.dmi.dbis.adam.main.SparkStartup

/**
 * adamtwo
 *
 * Ivan Giangreco
 * December 2015
 */
object EvaluationPerformanceStartup {
    def main(args : Array[String]) {
      SparkStartup

      val epprog = new EvaluationProgressiveQueryPerformer()
      epprog.init()
      epprog.start()

      val epindex = new EvaluationIndexQueryPerformer()
      epindex.init()
      epindex.start()

      val epseq = new EvaluationSequentialQueryPerformer()
      epseq.init()
      epseq.start()
    }
  }
