package ch.unibas.dmi.dbis.adam.evaluation

import java.io.{BufferedWriter, FileWriter, PrintWriter}
import java.util.Calendar

import ch.unibas.dmi.dbis.adam.http.grpc.RepartitionMessage

/**
  * Created by silvan on 08.07.16.
  */
trait EvaluationResultLogger {

  val fw = new FileWriter("results_" + Calendar.getInstance().getTime.toString + ".csv", true)
  val bw = new BufferedWriter(fw)
  val out = new PrintWriter(bw)

  def appendToResults(tuples: Int, dimensions: Int, partitions: Int, index: String, time: Float, k: Int = 0, noResults: Int = 0, partitioner: RepartitionMessage.Partitioner): Unit = {
    out.println(Calendar.getInstance().getTime() + "," + index + "," + tuples + "," + dimensions + "," + partitions + "," + time + "," + k + "," + noResults + "," + partitioner.name)
    out.flush()
  }
}
