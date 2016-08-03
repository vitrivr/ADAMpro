package ch.unibas.dmi.dbis.adam.evaluation

import java.io.{BufferedWriter, FileWriter, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Calendar

import ch.unibas.dmi.dbis.adam.http.grpc.RepartitionMessage

/**
  * Created by silvan on 08.07.16.
  */
trait EvaluationResultLogger {

  val sdf = new SimpleDateFormat("MMdd_HHmm")
  val fw = new FileWriter("results_" +sdf.format(Calendar.getInstance().getTime())+ ".csv", true)
  val bw = new BufferedWriter(fw)
  val out = new PrintWriter(bw)

  def setTuples(tuples: Int) = {
    this.logTuples = tuples
  }
  def setDimensions(dim: Int) = {
    logDim = dim
  }
  def setPartitions(part: Int) = {
    logPartitions = part
  }
  def setIndex(index: String) = {
    this.index = index
  }
  def setK(k: Int) = {
    this.logK = k
  }
  def setPartitioner(part: RepartitionMessage.Partitioner) = {
    this.partitioner = part
  }
  def setDropPerc(d :Double) = {
    this.dropPercentage =d
  }

  var logTuples = 0
  var logDim = 0
  var logPartitions = 0
  var index = ""
  var logK = 0
  var partitioner : RepartitionMessage.Partitioner = RepartitionMessage.Partitioner.SH
  var dropPercentage = 0.0

  def write(time: Float, noResults: Int, skipNoSkipK: Float, ratioK: Float, missingKTruth: Float) = {
    out.println(Calendar.getInstance().getTime() + "," + index + "," + logTuples + "," + logDim + "," + logPartitions + "," + time + "," + logK + "," + noResults + "," + partitioner.name+","+dropPercentage+","+skipNoSkipK+","+ratioK+","+missingKTruth)
    out.flush()
  }

  def appendToResults(tuples: Int, dimensions: Int, partitions: Int, index: String, time: Float, k: Int = 0, noResults: Int = 0, partitioner: RepartitionMessage.Partitioner, informationLoss: Double, dropPerc: Double, ratio: Float): Unit = {
    out.println(Calendar.getInstance().getTime() + "," + index + "," + tuples + "," + dimensions + "," + partitions + "," + time + "," + k + "," + noResults + "," + partitioner.name+","+dropPerc+","+informationLoss+","+ratio+",ALL")
    out.flush()
  }
}
