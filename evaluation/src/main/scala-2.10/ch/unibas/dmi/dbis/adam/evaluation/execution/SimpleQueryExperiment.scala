package ch.unibas.dmi.dbis.adam.evaluation.execution

import java.io.PrintWriter

import ch.unibas.dmi.dbis.adam.evaluation.client.EvaluationClient
import ch.unibas.dmi.dbis.adam.evaluation.config.IndexTypes

/**
  * adampro
  *
  * Ivan Giangreco
  * March 2016
  */
class SimpleQueryExperiment(client: EvaluationClient, entityname : String, collectionSize: Int, vectorSize: Int, indexes: Seq[IndexTypes.IndexType], k : Int, numberOfExperiments: Int) extends Experiment {

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
        "total time" + "," +
        "results" +
        "\n")
  }

  /**
    *
    * @param writer
    */
  override def run(writer: PrintWriter) = {
    //indexes
    (0 until numberOfExperiments).foreach {
      runID =>
        indexes.foreach {
          index =>
            var results: Seq[(Long, Float)] = null
            val runningTime = time {
              results = client.indexQuery(entityname, index, getRandomVector(vectorSize), k)
            }

            writer.write(
              System.nanoTime() + "," +
                collectionSize + "," +
                vectorSize + "," +
                index.name + "," +
                runningTime + "," +
                results.map(_._1).mkString("{", ";", "}") +
                "\n")
            writer.flush()
        }
    }

    //sequential
    (0 until numberOfExperiments).foreach {
      runID =>
        var results: Seq[(Long, Float)] = null
        val runningTime = time {
          results = client.sequentialQuery(entityname, getRandomVector(vectorSize), k)
        }

        writer.write(
          System.nanoTime() + "," +
            collectionSize + "," +
            vectorSize + "," +
            "seq" + "," +
            runningTime + "," +
            results.map(_._1).mkString("{", ";", "}") +
            "\n")
        writer.flush()
    }
  }

  /**
    *
    * @param writer
    */
  override def shutdown(writer: PrintWriter) = {}

  /**
    *
    * @return
    */
  override def name: String = collectionSize + "_" + vectorSize + "_" + indexes.mkString("-")
}


