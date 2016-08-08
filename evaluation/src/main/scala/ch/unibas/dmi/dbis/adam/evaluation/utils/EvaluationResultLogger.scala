package ch.unibas.dmi.dbis.adam.evaluation.utils

import java.io.{BufferedWriter, FileWriter, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Calendar

import ch.unibas.dmi.dbis.adam.http.grpc.RepartitionMessage
import ch.unibas.dmi.dbis.adam.http.grpc.RepartitionMessage.Partitioner

/**
  * Created by silvan on 08.07.16.
  */
trait EvaluationResultLogger {

  val sdf = new SimpleDateFormat("MMdd_HHmm")
  val fw = new FileWriter("results_" + sdf.format(Calendar.getInstance.getTime) + ".csv", true)
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

  def setDropPerc(d: Double) = {
    this.dropPercentage = d
  }

  def getPartitioner: Partitioner = partitioner

  var logTuples = 0
  var logDim = 0
  var logPartitions = 0
  var index = ""
  var logK = 0
  var partitioner: RepartitionMessage.Partitioner = RepartitionMessage.Partitioner.SH
  var dropPercentage = 0.0

  val seperator = "\t"

  /** http://stackoverflow.com/questions/11106886/scala-doubles-and-precision */
  def round(f: Float, p: Int): Float = {
    BigDecimal(f).setScale(p, BigDecimal.RoundingMode.HALF_UP).toFloat
  }

  out.println(String.format("%-29s", "time") + seperator + "idx" + seperator + String.format("%-7s", "Tuple") + seperator + "Dim" + seperator + "Par" + seperator + "time" +
    seperator + "k" + seperator + "#res    " + seperator + "partitioner" + seperator + "d%" + seperator + "mK " + seperator + "top-K Hits")
  out.flush()

  //TODO Maybe rewrite this to option map
  def write(time: Float, noResults: Int, missingKTruth: Float, topKNoSkip: Float) = {
    //http://stackoverflow.com/questions/9048132/scala-cant-recognize-which-method-to-call scala.Int is not a java.lang.Object
    val noResPadded = String.format("%08d", noResults: java.lang.Integer)
    val partitionerPadded = String.format("%-11s", partitioner.name)
    val topKNSPadded = String.format("%3.2f", round(topKNoSkip, 2): java.lang.Float)

    out.println(Calendar.getInstance.getTime + seperator + index + seperator + logTuples + seperator + logDim + seperator + logPartitions + seperator + time +
      seperator + logK + seperator + noResPadded + seperator + partitionerPadded + seperator + dropPercentage +
      seperator + String.format("%2.0f", round(missingKTruth, 2): java.lang.Float) + seperator + topKNSPadded)
    out.flush()
  }

  def appendToResults(tuples: Int, dimensions: Int, partitions: Int, index: String, time: Float, k: Int = 0, noResults: Int = 0, partitioner: RepartitionMessage.Partitioner, informationLoss: Double, dropPerc: Double, ratio: Float): Unit = {
    out.println(Calendar.getInstance.getTime + "," + index + "," + tuples + "," + dimensions + "," + partitions + "," + time + "," + k + "," + noResults + "," + partitioner.name + "," + dropPerc + "," + informationLoss + "," + ratio + ",ALL")
    out.flush()
  }
}
