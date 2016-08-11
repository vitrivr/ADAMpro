package ch.unibas.dmi.dbis.adam.evaluation.utils

import java.io.{BufferedWriter, FileWriter, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Calendar

import ch.unibas.dmi.dbis.adam.http.grpc.RepartitionMessage
import ch.unibas.dmi.dbis.adam.http.grpc.RepartitionMessage.Partitioner

/**
  * Created by silvan on 08.07.16.
  */
trait EvaluationResultLogger extends Logging {

  val sdf = new SimpleDateFormat("MMdd_HHmm")
  val fw = new FileWriter("results_" + sdf.format(Calendar.getInstance.getTime) + ".csv", true)
  val bw = new BufferedWriter(fw)
  val out = new PrintWriter(bw)

  def setTuples(tuples: Int) = {
    log.debug("Setting Tuples to " + tuples)
    this.logTuples = tuples
  }

  def setDimensions(dim: Int) = {
    log.debug("Setting Dimensions to " + dim)
    logDim = dim
  }

  def setPartitions(part: Int) = {
    log.debug("Setting partitions to " + part)
    logPartitions = part
  }

  def setIndex(index: String) = {
    log.debug("Setting index structure to " + index)
    this.index = index
  }

  def setK(k: Int) = {
    log.debug("Setting k to " + k)
    this.logK = k
  }

  def setPartitioner(part: RepartitionMessage.Partitioner) = {
    log.debug("Setting Partitioner to " + part)
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

  /** Header line. Not formatted in line with results, but that's life*/
  out.println("curr_time" + seperator + "idx" + seperator + "tuples" + seperator + "dimensions" + seperator + "partitions" + seperator + "time" +
    seperator + "k" + seperator + "no_res" + seperator + "partitioner" + seperator + "skipPercentage" + seperator + "topk" + seperator + "noskip_topk")
  out.flush()

  /** http://stackoverflow.com/questions/11106886/scala-doubles-and-precision */
  def round(f: Float, p: Int): Float = {
    BigDecimal(f).setScale(p, BigDecimal.RoundingMode.HALF_UP).toFloat
  }

  /** Pads some numbers for better readability */
  def write(time: Float, noResults: Int, topKSkip: Float, topKNoSkip: Float) = {
    val noResPadded = String.format("%08d", noResults: java.lang.Integer)
    val topKNSPadded = String.format("%3.2f", round(topKNoSkip, 2): java.lang.Float)

    out.println(Calendar.getInstance.getTime + seperator + index + seperator + logTuples + seperator + logDim + seperator + logPartitions + seperator + time +
      seperator + logK + seperator + noResPadded + seperator + partitioner + seperator + dropPercentage +
      seperator + round(topKSkip, 2)+ seperator + topKNSPadded)
    out.flush()
  }
}
