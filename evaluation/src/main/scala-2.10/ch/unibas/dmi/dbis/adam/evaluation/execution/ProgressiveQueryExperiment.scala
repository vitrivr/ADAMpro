package ch.unibas.dmi.dbis.adam.evaluation.execution

import java.io.PrintWriter

import ch.unibas.dmi.dbis.adam.evaluation.client.EvaluationClient
import ch.unibas.dmi.dbis.adam.evaluation.config.IndexTypes
import ch.unibas.dmi.dbis.adam.evaluation.config.IndexTypes.IndexType

/**
  * adampro
  *
  * Ivan Giangreco
  * March 2016
  */
class ProgressiveQueryExperiment(client: EvaluationClient, entityname : String, collectionSize: Int, vectorSize: Int, indexes: Seq[IndexTypes.IndexType], k : Int, numberOfExperiments: Int) extends Experiment {
  /**
    *
    * @param writer
    */
  override def startup(writer: PrintWriter) = {
    writer.write(
      "id" + "," +
        "collection size" + "," +
        "vector size" + "," +
        "index type" + "," +
        "end time" + "," +
        "start time" + "," +
        "results" +
        "\n")
  }

  override def run(writer: PrintWriter) = {
    //indexes
    (0 until numberOfExperiments).foreach {
      runID =>
        val runningTime = time {
          client.progressiveQuery(entityname, getRandomVector(vectorSize), k, onComplete(writer, System.nanoTime()))
        }
    }
  }

  def onComplete(writer: PrintWriter, startTime: Long)(confidence: Double, results: Seq[(Long, Float)], indextype: IndexType) {
    writer.write(
      System.nanoTime() + "," +
        collectionSize + "," +
        vectorSize + "," +
        indextype + "," +
        System.nanoTime() + "," +
        startTime + "," +
        results.map(_._1).mkString("{", ";", "}") +
        "\n")
    writer.flush()
  }

  /**
    *
    */
  override def shutdown(writer: PrintWriter) = {}

  /**
    *
    * @return
    */
  override def name: String = collectionSize + "_" + vectorSize + "_" + indexes.mkString("-")
}